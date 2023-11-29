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

package com.antgroup.geaflow.cluster.driver;

import com.antgroup.geaflow.cluster.common.AbstractContainer;
import com.antgroup.geaflow.cluster.common.ExecutionIdGenerator;
import com.antgroup.geaflow.cluster.exception.ComponentUncaughtExceptionHandler;
import com.antgroup.geaflow.cluster.executor.IPipelineExecutor;
import com.antgroup.geaflow.cluster.executor.PipelineExecutorContext;
import com.antgroup.geaflow.cluster.executor.PipelineExecutorFactory;
import com.antgroup.geaflow.cluster.protocol.IEvent;
import com.antgroup.geaflow.cluster.rpc.impl.DriverEndpoint;
import com.antgroup.geaflow.cluster.rpc.impl.PipelineMasterEndpoint;
import com.antgroup.geaflow.cluster.rpc.impl.RpcServiceImpl;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.common.utils.ProcessUtil;
import com.antgroup.geaflow.common.utils.ThreadUtil;
import com.antgroup.geaflow.pipeline.Pipeline;
import com.antgroup.geaflow.pipeline.callback.TaskCallBack;
import com.antgroup.geaflow.pipeline.service.PipelineService;
import com.antgroup.geaflow.pipeline.task.PipelineTask;
import com.antgroup.geaflow.shuffle.service.ShuffleManager;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Driver process.
 */
public class Driver extends AbstractContainer implements IDriver<IEvent, Boolean> {//表示驱动程序，用于执行Pipeline

    private static final Logger LOGGER = LoggerFactory.getLogger(Driver.class);
    private static final String DRIVER_PREFIX = "driver-";//用于生成驱动程序的名称前缀
    private static final String DRIVER_EXECUTOR = "driver-executor";//用于指定驱动程序的执行器的名称
    private static final AtomicInteger pipelineTaskIdGenerator = new AtomicInteger(0);//用于生成唯一的任务ID。

    private DriverEventDispatcher eventDispatcher;//用于事件分发
    private DriverContext driverContext;//用于存储驱动程序的上下文信息
    private ExecutorService executorService;//用于执行Pipeline的线程池

    public Driver() {
        this(0);
    }

    public Driver(int rpcPort) {
        super(rpcPort);
    }
    //Driver类有两个构造函数，一个无参数构造函数，一个带有rpcPort参数的构造函数。
    //在构造函数内部，调用父类的构造函数，并初始化一些成员变量

    @Override
    public void init(DriverContext driverContext) {
        super.init(driverContext.getId(), DRIVER_PREFIX, driverContext.getConfig());
        this.driverContext = driverContext;
        this.eventDispatcher = new DriverEventDispatcher();
        this.executorService = Executors.newFixedThreadPool(
            1,
            ThreadUtil.namedThreadFactory(true, DRIVER_EXECUTOR, new ComponentUncaughtExceptionHandler()));

        ExecutionIdGenerator.init(id);
        ShuffleManager.getInstance().initShuffleMaster();
        if (driverContext.getPipeline() != null) {
            LOGGER.info("driver {} execute pipeline from recovered context", name);
            executorService.execute(() -> executePipelineInternal(driverContext.getPipeline()));
        }
        registerToMaster();
        registerHAService();
        LOGGER.info("driver {} init finish", name);
    }
    //该方法用于初始化驱动程序。
    //在方法内部，初始化eventDispatcher、driverContext和executorService。
    //调用ExecutionIdGenerator.init来初始化执行ID生成器。
    //初始化Shuffle管理器（ShuffleManager）。
    //如果driverContext中存在Pipeline，则在单独的线程中执行该Pipeline。
    //注册驱动程序到主控节点（PipelineMaster）。
    //注册高可用服务。

    @Override
    protected void startRpcService() {
        this.rpcService = new RpcServiceImpl(rpcPort, configuration);
        this.rpcService.addEndpoint(new DriverEndpoint(this));
        this.rpcService.addEndpoint(new PipelineMasterEndpoint(this));
        this.rpcPort = rpcService.startService();
    }
    //该方法用于启动RPC服务。
    //创建一个RpcServiceImpl对象，并添加驱动程序端点（DriverEndpoint）和主控节点端点（PipelineMasterEndpoint）。
    //启动RPC服务。

    @Override
    public Boolean executePipeline(Pipeline pipeline) {
        LOGGER.info("driver {} execute pipeline {}", name, pipeline);
        Future<Boolean> future = executorService.submit(() -> executePipelineInternal(pipeline));
        try {
            return future.get();
        } catch (Throwable e) {
            throw new GeaflowRuntimeException(e);
        }
    }
    //该方法用于执行指定的Pipeline。
    //方法参数包括要执行的Pipeline。
    //通过executorService提交一个任务，执行executePipelineInternal方法，并返回Future对象。
    //等待Future的执行结果，如果有异常则抛出异常。

    public Boolean executePipelineInternal(Pipeline pipeline) {
        try {
            LOGGER.info("start execute pipeline {}", pipeline);
            driverContext.addPipeline(pipeline);
            driverContext.checkpoint(new DriverContext.PipelineCheckpointFunction());

            IPipelineExecutor pipelineExecutor = PipelineExecutorFactory.createPipelineExecutor();
            PipelineExecutorContext executorContext = new PipelineExecutorContext(name,
                eventDispatcher, configuration, pipelineTaskIdGenerator);
            pipelineExecutor.init(executorContext);
            pipelineExecutor.register(pipeline.getViewDescMap());

            List<PipelineTask> pipelineTaskList = pipeline.getPipelineTaskList();
            List<TaskCallBack> taskCallBackList = pipeline.getPipelineTaskCallbacks();
            for (int i = 0, size = pipelineTaskList.size(); i < size; i++) {
                if (driverContext.getFinishedPipelineTasks() == null || !driverContext.getFinishedPipelineTasks().contains(i)) {
                    pipelineExecutor.runPipelineTask(pipelineTaskList.get(i),
                        taskCallBackList.get(i));
                    driverContext.addFinishedPipelineTask(i);
                    driverContext.checkpoint(new DriverContext.PipelineTaskCheckpointFunction());
                }
            }

            List<PipelineService> pipelineServices = pipeline.getPipelineServices();
            for (PipelineService pipelineService : pipelineServices) {
                LOGGER.info("execute service");
                pipelineExecutor.startPipelineService(pipelineService);
            }
            LOGGER.info("finish execute pipeline {}", pipeline);
            return true;
        } catch (Throwable e) {
            LOGGER.error("driver exception", e);
            throw e;
        }
    }
    //该方法用于执行指定的Pipeline。
    //在方法内部，首先初始化执行器（IPipelineExecutor）和执行器上下文（PipelineExecutorContext）。
    //注册Pipeline的视图描述信息。
    //遍历Pipeline的任务列表，运行每个任务，同时记录已完成的任务。
    //启动Pipeline的服务。
    //返回执行结果。
    @Override
    public Boolean process(IEvent input) {
        LOGGER.info("{} process event {}", name, input);
        eventDispatcher.dispatch(input);
        return true;
    }
    //该方法用于处理事件。
    //事件被传递给事件分发器（eventDispatcher

    @Override
    public void close() {
        super.close();
        executorService.shutdownNow();
        LOGGER.info("driver {} closed", name);
    }

    protected DriverInfo buildComponentInfo() {
        DriverInfo driverInfo = new DriverInfo();
        driverInfo.setId(id);
        driverInfo.setName(name);
        driverInfo.setHost(ProcessUtil.getHostIp());
        driverInfo.setPid(ProcessUtil.getProcessId());
        driverInfo.setRpcPort(rpcPort);
        return driverInfo;
    }
    //该方法用于构建驱动程序的信息。
    //返回一个包含驱动程序信息的DriverInfo对象。
}
