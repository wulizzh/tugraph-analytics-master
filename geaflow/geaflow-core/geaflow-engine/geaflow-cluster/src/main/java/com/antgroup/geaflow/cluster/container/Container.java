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

package com.antgroup.geaflow.cluster.container;

import com.antgroup.geaflow.cluster.collector.EmitterService;
import com.antgroup.geaflow.cluster.common.AbstractContainer;
import com.antgroup.geaflow.cluster.fetcher.FetcherService;
import com.antgroup.geaflow.cluster.protocol.ICommand;
import com.antgroup.geaflow.cluster.protocol.IEvent;
import com.antgroup.geaflow.cluster.protocol.OpenContainerEvent;
import com.antgroup.geaflow.cluster.protocol.OpenContainerResponseEvent;
import com.antgroup.geaflow.cluster.rpc.impl.ContainerEndpoint;
import com.antgroup.geaflow.cluster.rpc.impl.RpcServiceImpl;
import com.antgroup.geaflow.cluster.task.service.TaskService;
import com.antgroup.geaflow.cluster.worker.Dispatcher;
import com.antgroup.geaflow.cluster.worker.DispatcherService;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.common.utils.ProcessUtil;
import com.antgroup.geaflow.shuffle.service.ShuffleManager;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Container extends AbstractContainer implements IContainer<IEvent, IEvent> {//Container类是容器的实现，用于托管任务

    private static final Logger LOGGER = LoggerFactory.getLogger(Container.class);

    private static final String CONTAINER_NAME_PREFIX = "container-";

    private ContainerContext containerContext;//容器上下文，用于存储容器的配置信息
    private Dispatcher dispatcher;//调度器，用于处理任务调度。
    protected FetcherService fetcherService;//用于数据获取的服务。
    protected EmitterService emitterService;//用于数据发射的服务
    protected TaskService workerService;//任务服务，负责执行任务
    protected DispatcherService dispatcherService;//调度器服务，用于处理任务调度。

    public Container() {
        this(0);
    }

    public Container(int rpcPort) {
        super(rpcPort);
    }
    //Container类有一个带有rpcPort参数的构造函数，用于初始化容器。
    //在构造函数内部，调用父类（AbstractContainer）的构造函数，传递rpcPort参数。
    @Override
    public void init(ContainerContext containerContext) {
        try {
            this.containerContext = containerContext;
            super.init(containerContext.getId(), CONTAINER_NAME_PREFIX, containerContext.getConfig());
            registerToMaster();
            LOGGER.info("container {} init finish", name);
        } catch (Throwable t) {
            LOGGER.error("init container err", t);
            throw new GeaflowRuntimeException(t);
        }
    }
    //该方法用于初始化容器。
    //在方法内部，存储传递的容器上下文到containerContext成员变量。
    //调用父类的init方法来初始化容器的基本信息。
    //注册容器到Master，用于向Master报告容器的存在。

    @Override
    protected void startRpcService() {
        this.rpcService = new RpcServiceImpl(rpcPort, configuration);
        this.rpcService.addEndpoint(new ContainerEndpoint(this));
        this.rpcPort = rpcService.startService();
    }
    //该方法用于启动容器的RPC服务。
    //创建RpcServiceImpl实例，并添加ContainerEndpoint用于处理容器相关的RPC请求。
    //启动RPC服务并获取分配的端口。

    public OpenContainerResponseEvent open(OpenContainerEvent event) {
        try {
            int num = event.getExecutorNum();
            Preconditions.checkArgument(num > 0, "worker num should > 0");
            LOGGER.info("open container {} with {} executors", name, num);

            this.fetcherService = new FetcherService(num, configuration);
            this.emitterService = new EmitterService(num, configuration);
            this.workerService = new TaskService(id, num,
                configuration, metricGroup, fetcherService, emitterService);
            this.dispatcher = new Dispatcher(workerService);
            this.dispatcherService = new DispatcherService(dispatcher);

            // start task service
            this.fetcherService.start();
            this.emitterService.start();
            this.workerService.start();
            this.dispatcherService.start();

            if (containerContext.getReliableEvents() != null) {
                for (IEvent reliableEvent : containerContext.getReliableEvents()) {
                    LOGGER.info("{} replay event {}", name, reliableEvent);
                    this.dispatcher.add((ICommand) reliableEvent);
                }
            }
            registerHAService();
            return new OpenContainerResponseEvent(id, 0);
        } catch (Throwable throwable) {
            LOGGER.error("{} open error", name, throwable);
            throw throwable;
        }
    }
    //该方法用于打开容器，并为容器分配任务执行器（executors）。
    //参数event包含要分配的执行器数量。
    //在方法内部，校验执行器数量，然后初始化数据获取服务、数据发射服务、任务服务和调度器。
    //启动数据获取服务、数据发射服务、任务服务和调度器。
    //如果存在可靠事件（reliableEvents），将它们重新添加到调度器中以重播它们。
    //注册容器到HA服务。
    //返回OpenContainerResponseEvent，表示容器已经成功打开。

    @Override
    public IEvent process(IEvent input) {
        LOGGER.info("{} process event {}", name, input);
        try {
            this.containerContext.addEvent(input);
            this.containerContext.checkpoint(new ContainerContext.EventCheckpointFunction());
            this.dispatcher.add((ICommand) input);
            return null;
        } catch (Throwable throwable) {
            LOGGER.error("{} process error", name, throwable);
            throw throwable;
        }
    }
    //该方法用于处理事件，具体用途是将事件添加到调度器中执行。
    //参数input表示要处理的事件。
    //在方法内部，将事件添加到容器上下文、检查点（checkpoint）并添加到调度器中。
    //返回null，表示没有返回事件。
    @Override
    public void close() {
        super.close();
        if (fetcherService != null) {
            fetcherService.shutdown();
        }
        if (workerService != null) {
            workerService.shutdown();
        }
        if (dispatcherService != null) {
            dispatcherService.shutdown();
        }
        if (emitterService != null) {
            emitterService.shutdown();
        }
        LOGGER.info("container {} closed", name);
    }

    protected ContainerInfo buildComponentInfo() {
        ContainerInfo containerInfo = new ContainerInfo();
        containerInfo.setId(id);
        containerInfo.setName(name);
        containerInfo.setPid(ProcessUtil.getProcessId());
        containerInfo.setHost(ProcessUtil.getHostIp());
        containerInfo.setRpcPort(rpcPort);
        containerInfo.setShufflePort(ShuffleManager.getInstance().getShufflePort());
        return containerInfo;
    }
    //该方法用于构建容器的信息，包括容器的ID、名称、进程ID、主机IP、RPC端口和Shuffle端口。
    //返回ContainerInfo对象
}
