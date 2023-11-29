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

package com.antgroup.geaflow.cluster.common;

import com.antgroup.geaflow.cluster.rpc.RpcClient;
import com.antgroup.geaflow.cluster.rpc.impl.RpcServiceImpl;
import com.antgroup.geaflow.cluster.system.ClusterMetaStore;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.utils.ProcessUtil;
import com.antgroup.geaflow.ha.service.HAServiceFactory;
import com.antgroup.geaflow.ha.service.IHAService;
import com.antgroup.geaflow.ha.service.ResourceData;
import com.antgroup.geaflow.metrics.common.MetricGroupRegistry;
import com.antgroup.geaflow.metrics.common.api.MetricGroup;
import com.antgroup.geaflow.shuffle.service.ShuffleManager;
import com.antgroup.geaflow.stats.collector.StatsCollectorFactory;

public abstract class AbstractComponent {//它作为GeaFlow集群中不同组件的基类

    protected int id;//组件的唯一标识符。
    protected String name;//组件的名称
    protected String masterId;//主节点的唯一标识符。
    protected int rpcPort;//组件的RPC通信端口。

    protected Configuration configuration;//一个Configuration对象，用于存储配置信息
    protected IHAService haService;//一个IHAService类型的对象，用于处理高可用相关的任务。
    protected RpcServiceImpl rpcService;//一个RpcServiceImpl类型的对象，用于提供RPC服务
    protected MetricGroup metricGroup;//一个MetricGroup类型的对象，用于度量统计
    protected MetricGroupRegistry metricGroupRegistry;//一个MetricGroupRegistry类型的对象，用于度量统计注册。

    public AbstractComponent() {
    }

    public AbstractComponent(int rpcPort) {
        this.rpcPort = rpcPort;
    }

    public void init(int id, String name, Configuration configuration) {
        this.id = id;
        this.name = name;
        this.configuration = configuration;
        this.masterId = configuration.getMasterId();

        this.metricGroupRegistry = MetricGroupRegistry.getInstance(configuration);
        this.metricGroup = metricGroupRegistry.getMetricGroup();
        this.haService = HAServiceFactory.getService(configuration);

        RpcClient.init(configuration);//初始化了RPC客户端（RpcClient）
        ClusterMetaStore.init(id, configuration);//初始化了集群元数据存储（ClusterMetaStore）
        StatsCollectorFactory.init(configuration);//初始化了统计收集工厂（StatsCollectorFactory）
    }

    protected abstract void startRpcService();

    protected void registerHAService() {
        ResourceData resourceData = new ResourceData();
        resourceData.setProcessId(ProcessUtil.getProcessId());
        resourceData.setHost(ProcessUtil.getHostIp());
        resourceData.setRpcPort(rpcPort);
        ShuffleManager shuffleManager = ShuffleManager.getInstance();
        if (shuffleManager != null) {
            resourceData.setShufflePort(shuffleManager.getShufflePort());
        }
        haService.register(name, resourceData);
    }
    //该方法用于向高可用服务注册组件的信息。
    //首先创建了一个ResourceData对象，用于描述组件的资源信息，包括进程ID、主机IP、RPC端口和Shuffle端口。
    //获取了Shuffle管理器对象，并根据其信息设置了shufflePort。
    //最后，调用高可用服务的register方法，将组件的名称和资源数据注册到高可用服务中。

    public void close() {
        if (haService != null) {
            haService.close();
        }
        if (this.rpcService != null) {
            this.rpcService.stopService();
        }
        ClusterMetaStore.close();
    }
    //该方法用于关闭组件，释放资源。
    //首先检查高可用服务对象（haService）是否存在，如果存在则调用其close方法关闭高可用服务。
    //然后检查RPC服务对象（rpcService）是否存在，如果存在则调用其stopService方法停止RPC服务。
    //最后，关闭集群元数据存储（ClusterMetaStore）

    public void waitTermination() {
        rpcService.waitTermination();
    }

}
