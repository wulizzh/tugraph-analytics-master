/*
 * Copyright 2023 AntGroup CO., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.antgroup.geaflow.cluster.resourcemanager;

import com.antgroup.geaflow.cluster.clustermanager.ClusterContext;
import com.antgroup.geaflow.cluster.clustermanager.ContainerExecutorInfo;
import com.antgroup.geaflow.cluster.clustermanager.ExecutorRegisterException;
import com.antgroup.geaflow.cluster.clustermanager.ExecutorRegisteredCallback;
import com.antgroup.geaflow.cluster.clustermanager.IClusterManager;
import com.antgroup.geaflow.cluster.resourcemanager.allocator.IAllocator;
import com.antgroup.geaflow.cluster.resourcemanager.allocator.RoundRobinAllocator;
import com.antgroup.geaflow.cluster.system.ClusterMetaStore;
import com.antgroup.geaflow.common.errorcode.RuntimeErrors;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.common.utils.SleepUtils;
import com.google.common.annotations.VisibleForTesting;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultResourceManager implements IResourceManager, ExecutorRegisteredCallback, Serializable {//是资源管理器的默认实现，实现了IResourceManager接口。

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultResourceManager.class);

    private static final String OPERATION_REQUIRE = "require";
    private static final String OPERATION_RELEASE = "release";
    private static final String OPERATION_ALLOCATE = "allocate";

    private static final int DEFAULT_SLEEP_MS = 10;
    private static final int MAX_REQUIRE_RETRY_TIMES = 1;
    private static final int MAX_RELEASE_RETRY_TIMES = 100;

    private final AtomicReference<Throwable> allocateWorkerErr = new AtomicReference<>();//用于存储分配工作器的错误信息
    private final AtomicInteger pendingWorkerCounter = new AtomicInteger(0);//用于跟踪待处理的工作器数量。
    private final AtomicBoolean resourceLock = new AtomicBoolean(true);//用于控制资源管理器的并发访问。
    private final AtomicBoolean recovering = new AtomicBoolean(false);//用于标识资源管理器是否正在恢复状态。
    private final AtomicBoolean inited = new AtomicBoolean(false);//用于标识资源管理器是否已初始化
    private final Map<IAllocator.AllocateStrategy, IAllocator<String, WorkerInfo>> allocators = new HashMap<>();//用于存储分配策略和分配器的映射
    protected final IClusterManager clusterManager;//用于与集群管理器进行通信的接口
    protected final ClusterMetaStore metaKeeper;//用于存储集群元数据的对象

    private final Map<WorkerInfo.WorkerId, WorkerInfo> availableWorkers = new HashMap<>();//用于存储可用的工作器信息。
    private final Map<String, ResourceSession> sessions = new HashMap<>();//用于存储资源会话信息

    public DefaultResourceManager(IClusterManager clusterManager) {
        this.clusterManager = clusterManager;
        this.metaKeeper = ClusterMetaStore.getInstance();
    }
    //DefaultResourceManager类有一个带有IClusterManager参数的构造函数，用于初始化资源管理器。
    //在构造函数内部，初始化clusterManager和metaKeeper。
    //定义了常量字符串，用于标识不同的资源操作类型（require、release、allocate）和一些默认配置。

    @Override
    public void init(ResourceManagerContext context) {
        this.allocators.put(IAllocator.AllocateStrategy.ROUND_ROBIN, new RoundRobinAllocator());
        ClusterContext clusterContext = context.getClusterContext();
        clusterContext.addExecutorRegisteredCallback(this);

        boolean isRecover = context.isRecover();
        this.recovering.set(isRecover);
        int workerNum = clusterContext.getClusterConfig().getContainerNum()
            * clusterContext.getClusterConfig().getContainerWorkerNum();
        this.pendingWorkerCounter.set(workerNum);
        LOGGER.info("init worker number {}, isRecover {}", workerNum, isRecover);
        if (isRecover) {
            this.recover();
        } else {
            this.clusterManager.allocateWorkers(workerNum);//向集群管理器请求分配工作器。
        }
        this.inited.set(true);
        LOGGER.info("init worker manager finish");
    }
    //该方法用于初始化资源管理器。
    //在方法内部，初始化分配器策略和回调函数。
    //根据传递的配置信息，设置是否处于恢复状态，并初始化待处理的工作器数量。
    //如果是恢复状态，执行恢复操作，否则向集群管理器请求分配工作器。
    //设置资源管理器为已初始化状态

    @Override
    public RequireResponse requireResource(RequireResourceRequest requireRequest) {
        String requireId = requireRequest.getRequireId();
        int requiredNum = requireRequest.getRequiredNum();
        if (this.sessions.containsKey(requireId)) {
            Map<WorkerInfo.WorkerId, WorkerInfo> sessionWorkers = this.sessions.get(requireId).getWorkers();
            if (requiredNum != sessionWorkers.size()) {
                String msg = "require number mismatch, old " + sessionWorkers.size() + " new " + requiredNum;
                LOGGER.error("[{}] require from session err: {}", requireId, msg);
                return RequireResponse.fail(requireId, msg);
            }
            List<WorkerInfo> workers = new ArrayList<>(sessionWorkers.values());
            LOGGER.info("[{}] require from session with {} worker", requireId, workers.size());
            return RequireResponse.success(requireId, workers);
        }
        if (requiredNum <= 0) {
            String msg = RuntimeErrors.INST.resourceIllegalRequireNumError("illegal num " + requiredNum);
            LOGGER.error("[{}] {}", requireId, msg);
            return RequireResponse.fail(requireId, msg);
        }
        if (this.recovering.get()) {
            String msg = "resource manager still recovering";
            LOGGER.warn("[{}] {}", requireId, msg);
            return RequireResponse.fail(requireId, msg);
        }
        if (this.pendingWorkerCounter.get() > 0) {
            String msg = "some worker still pending creation";
            LOGGER.warn("[{}] {}", requireId, msg);
            return RequireResponse.fail(requireId, msg);
        }

        Optional<List<WorkerInfo>> optional = this.withLock(OPERATION_REQUIRE, num -> {
            if (this.availableWorkers.size() < num) {
                LOGGER.warn("[{}] require {}, available {}, return empty",
                    requireId, num, this.availableWorkers.size());
                return Collections.emptyList();
            }
            IAllocator.AllocateStrategy strategy = requireRequest.getAllocateStrategy();
            List<WorkerInfo> allocated = this.allocators.get(strategy)
                .allocate(this.availableWorkers.values(), num);
            for (WorkerInfo worker : allocated) {
                WorkerInfo.WorkerId workerId = worker.generateWorkerId();
                this.availableWorkers.remove(workerId);
                ResourceSession session = this.sessions.computeIfAbsent(requireId, ResourceSession::new);
                session.addWorker(workerId, worker);
            }
            LOGGER.info("[{}] require {} allocated {} available {}",
                requireId, num, allocated.size(), this.availableWorkers.size());
            if (!allocated.isEmpty()) {
                this.persist();
            }
            return allocated;
        }, requiredNum, MAX_REQUIRE_RETRY_TIMES);
        List<WorkerInfo> allocated = optional.orElse(Collections.emptyList());
        return RequireResponse.success(requireId, allocated);
    }
    //该方法用于请求资源，指定要求的工作器数量和分配策略。
    //方法参数包括要求的资源ID和要求的工作器数量。
    //如果资源ID已存在于资源会话中，直接返回会话中的工作器信息。
    //如果要求的工作器数量小于等于0，返回错误响应。
    //如果资源管理器正在恢复状态，返回错误响应。
    //如果还有待处理的工作器，返回错误响应。
    //否则，执行工作器的分配操作，根据分配策略从可用工作器中分配工作器，并将分配的工作器添加到资源会话中

    @Override
    public ReleaseResponse releaseResource(ReleaseResourceRequest releaseRequest) {
        String releaseId = String.valueOf(releaseRequest.getReleaseId());
        if (!this.sessions.containsKey(releaseId)) {
            String msg = "release fail, session not exists: " + releaseId;
            LOGGER.error(msg);
            return ReleaseResponse.fail(releaseId, msg);
        }
        int expectSize = this.sessions.get(releaseId).getWorkers().size();
        int actualSize = releaseRequest.getWorkers().size();
        if (expectSize != actualSize) {
            String msg = String.format("release fail, worker num of session %s mismatch, expected %d, actual %d",
                releaseId, expectSize, actualSize);
            LOGGER.error(msg);
            return ReleaseResponse.fail(releaseId, msg);
        }
        Optional<Boolean> optional = this.withLock(OPERATION_RELEASE, workers -> {
            for (WorkerInfo worker : workers) {
                WorkerInfo.WorkerId workerId = worker.generateWorkerId();
                this.availableWorkers.put(workerId, worker);
                if (!this.sessions.get(releaseId).removeWorker(workerId)) {
                    String msg = String.format("worker %s not exists in session %s", workerId, releaseId);
                    LOGGER.error(msg);
                    throw new GeaflowRuntimeException(msg);
                }
            }
            this.sessions.remove(releaseId);
            LOGGER.info("[{}] release {} available {}",
                releaseId, workers.size(), this.availableWorkers.size());
            this.persist();
            return true;
        }, releaseRequest.getWorkers(), MAX_RELEASE_RETRY_TIMES);

        if (!optional.orElse(false)) {
            String msg = "release fail after " + MAX_RELEASE_RETRY_TIMES + " times";
            LOGGER.error(msg);
            return ReleaseResponse.fail(releaseId, msg);
        }
        return ReleaseResponse.success(releaseId);
    }
    //该方法用于释放资源，指定要释放的工作器信息。
    //方法参数包括释放的资源ID和工作器信息。
    //如果资源会话中不存在指定的资源ID，返回错误响应。
    //检查释放的工作器数量是否与会话中的工作器数量匹配，不匹配则返回错误响应。
    //否则，执行工作器的释放操作，将工作器放回可用工作器列表中。

    @Override
    public void onSuccess(ContainerExecutorInfo containerExecutorInfo) {
        this.waitForInit();
        this.withLock(OPERATION_ALLOCATE, container -> {
            String containerName = container.getContainerName();
            String host = container.getHost();
            int rpcPort = container.getRpcPort();
            int shufflePort = container.getShufflePort();
            int processId = container.getProcessId();
            List<Integer> executorIds = container.getExecutorIds();
            onRegister(containerName, host, rpcPort, shufflePort, processId, executorIds);
            return true;
        }, containerExecutorInfo, Integer.MAX_VALUE);
    }
    //当工作器注册成功时，调用该方法。
    //方法参数包括容器执行器信息，包括容器名称、主机、RPC端口、Shuffle端口、进程ID和执行器ID列表。
    //在方法内部，处理工作器的注册信息，将工作器添加到可用工作器列表中。
    //更新待处理的工作器数量。

    private void onRegister(String containerName,
                            String host,
                            int rpcPort,
                            int shufflePort,
                            int processId,
                            List<Integer> executorIds) {
        for (Integer workerIndex : executorIds) {
            WorkerInfo.WorkerId workerId = new WorkerInfo.WorkerId(containerName, workerIndex);
            WorkerInfo worker = null;
            if (this.availableWorkers.containsKey(workerId)) {
                worker = this.availableWorkers.get(workerId);
            }
            for (ResourceSession session : this.sessions.values()) {
                if (session.getWorkers().containsKey(workerId)) {
                    worker = session.getWorkers().get(workerId);
                }
            }
            if (worker == null) {
                worker = WorkerInfo.build(
                    host, rpcPort, shufflePort, processId, workerIndex, containerName);
                this.availableWorkers.put(worker.generateWorkerId(), worker);
                int pending = this.pendingWorkerCounter.addAndGet(-1);
                LOGGER.info("allocate {} worker from cluster manager container {}, host {}, "
                        + "processId {}, workerIndex {}, pending {}",
                    executorIds.size(), containerName, host, processId, workerIndex, pending);
            } else {
                worker.setHost(host);
                worker.setProcessId(processId);
                worker.setRpcPort(rpcPort);
                worker.setShufflePort(shufflePort);
                int pending = this.pendingWorkerCounter.get();
                LOGGER.info("recover {} worker from cluster manager container {}, host {}, "
                        + "processId {}, workerIndex {}, pending {}",
                    executorIds.size(), containerName, host, processId, workerIndex, pending);
            }
        }
        int pending = this.pendingWorkerCounter.get();
        int used = this.sessions.values().stream().mapToInt(s -> s.getWorkers().size()).sum();
        if (pending <= 0) {
            this.recovering.set(false);
            LOGGER.info("register worker over, available/used : {}/{}, pending {}",
                this.availableWorkers.size(), used, pending);
        } else {
            LOGGER.info("still pending : {}, available/used : {}/{}",
                pending, this.availableWorkers.size(), used);
        }
        persist();
    }


    @Override
    public void onFailure(ExecutorRegisterException e) {
        LOGGER.error("create worker err", e);
        this.allocateWorkerErr.compareAndSet(null, e);
    }
    //当工作器注册失败时，调用该方法。
    //方法参数包括执行器注册异常。
    //在方法内部，记录工作器注册失败的异常信息。

    private <T, R> Optional<R> withLock(String operation, Function<T, R> function, T input, int maxRetryTimes) {
        this.checkError();
        try {
            int retry = 0;
            while (!this.resourceLock.compareAndSet(true, false)) {
                SleepUtils.sleepMilliSecond(DEFAULT_SLEEP_MS);
                retry++;
                if (retry >= maxRetryTimes) {
                    LOGGER.warn("[{}] lock not ready, return empty", operation);
                    return Optional.empty();
                }
                if (retry % 100 == 0) {
                    LOGGER.warn("[{}] lock not ready after {} times", operation, retry);
                }
            }
            return Optional.of(function.apply(input));
        } finally {
            this.resourceLock.set(true);
        }
    }
    //该方法用于在资源管理器上加锁执行指定的操作。
    //参数包括操作类型（operation）、操作函数（function）、输入参数（input）和最大重试次数。
    //方法内部尝试获取锁，如果锁已被占用，则等待一段时间后重试。
    //一旦获得锁，执行指定的操作函数，然后释放锁。
    //如果操作成功，返回操作结果；如果操作失败或达到最大重试次数，返回空（Optional.empty()）。

    private void persist() {
        final long start = System.currentTimeMillis();
        List<WorkerInfo> available = new ArrayList<>(this.availableWorkers.values());
        List<ResourceSession> sessions = new ArrayList<>(this.sessions.values());
        int used = sessions.stream().mapToInt(s -> s.getWorkers().size()).sum();
        this.metaKeeper.saveWorkers(new WorkerSnapshot(available, sessions));
        this.metaKeeper.flush();
        LOGGER.info("persist {}/{} workers costs {}ms",
            this.availableWorkers.size(), used, System.currentTimeMillis() - start);
    }
    //该方法用于将工作器信息和会话信息持久化到存储中。
    //将可用工作器和会话列表保存到集群元数据存储中。

    private void recover() {
        final long start = System.currentTimeMillis();
        WorkerSnapshot workerSnapshot = this.metaKeeper.getWorkers();
        List<WorkerInfo> available = workerSnapshot.getAvailableWorkers();
        List<ResourceSession> sessions = workerSnapshot.getSessions();
        for (WorkerInfo worker : available) {
            WorkerInfo.WorkerId workerId = worker.generateWorkerId();
            this.availableWorkers.put(workerId, worker);
        }
        int usedWorkerNum = 0;
        for (ResourceSession session : sessions) {
            String sessionId = session.getId();
            this.sessions.put(sessionId, session);
            usedWorkerNum += session.getWorkers().size();
        }
        int availableWorkerNum = this.availableWorkers.size();

        this.pendingWorkerCounter.addAndGet(- availableWorkerNum - usedWorkerNum);
        LOGGER.info("recover {}/{} workers, pending {}, costs {}ms",
            availableWorkerNum, usedWorkerNum, pendingWorkerCounter.get(), System.currentTimeMillis() - start);
        if (this.pendingWorkerCounter.get() <= 0) {
            this.recovering.set(false);
            LOGGER.info("recover worker over, available/used : {}/{}",
                this.availableWorkers.size(), usedWorkerNum);
        }
    }
    //该方法用于恢复工作器信息和会话信息。
    //从存储中加载可用工作器和会话列表，进行恢复。
    //更新待处理的工作器数量和恢复状态。

    private void waitForInit() {
        int count = 0;
        while (!this.inited.get()) {
            count++;
            if (count % 100 == 0) {
                LOGGER.warn("resource manager not inited, wait {}ms and retry", DEFAULT_SLEEP_MS);
            }
            SleepUtils.sleepMilliSecond(DEFAULT_SLEEP_MS);
        }
    }

    @VisibleForTesting
    protected AtomicInteger getPendingWorkerCounter() {
        return this.pendingWorkerCounter;
    }

    @VisibleForTesting
    protected AtomicBoolean getResourceLock() {
        return this.resourceLock;
    }

    private void checkError() {
        Throwable firstException = this.allocateWorkerErr.get();
        if (firstException != null) {
            throw new GeaflowRuntimeException(firstException);
        }
    }

}
