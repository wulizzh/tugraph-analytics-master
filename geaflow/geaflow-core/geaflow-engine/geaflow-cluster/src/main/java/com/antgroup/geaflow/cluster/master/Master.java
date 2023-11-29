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

package com.antgroup.geaflow.cluster.master;

import com.antgroup.geaflow.cluster.clustermanager.ClusterContext;
import com.antgroup.geaflow.cluster.clustermanager.ClusterInfo;
import com.antgroup.geaflow.cluster.clustermanager.IClusterManager;
import com.antgroup.geaflow.cluster.common.AbstractComponent;
import com.antgroup.geaflow.cluster.heartbeat.HeartbeatManager;
import com.antgroup.geaflow.cluster.resourcemanager.IResourceManager;
import com.antgroup.geaflow.cluster.resourcemanager.ResourceManagerContext;
import com.antgroup.geaflow.cluster.resourcemanager.ResourceManagerFactory;
import com.antgroup.geaflow.cluster.rpc.RpcAddress;
import com.antgroup.geaflow.cluster.rpc.impl.MasterEndpoint;
import com.antgroup.geaflow.cluster.rpc.impl.ResourceManagerEndpoint;
import com.antgroup.geaflow.cluster.rpc.impl.RpcServiceImpl;
import com.antgroup.geaflow.cluster.web.HttpServer;
import com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys;
import com.antgroup.geaflow.common.utils.ProcessUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Master extends AbstractComponent implements IMaster {//用于实现GeaFlow集群中的主节点。它扩展了AbstractComponent类，因此继承了AbstractComponent类中的属性和方法
    private static final Logger LOGGER = LoggerFactory.getLogger(Master.class);

    private IResourceManager resourceManager;//一个IResourceManager类型的对象，用于资源管理
    private IClusterManager clusterManager;//一个IClusterManager类型的对象，用于集群管理
    private HeartbeatManager heartbeatManager;//一个HeartbeatManager类型的对象，用于处理心跳信息。
    private RpcAddress masterAddress;//一个RpcAddress类型的对象，用于存储主节点的RPC地址信息
    private HttpServer httpServer;//一个HttpServer类型的对象，用于处理HTTP请求

    public Master() {
        this(0);
    }

    public Master(int rpcPort) {
        super(rpcPort);
    }//构造函数中通过调用super(rpcPort)来调用父类（AbstractComponent）的构造函数，以设置rpcPort属性。
    //Master类有两个构造函数，一个不带参数，另一个带有参数rpcPort。
    // 它们用于初始化rpcPort属性，这是主节点的RPC通信端口。
    @Override
    public void init(MasterContext context) {//该方法用于初始化主节点的各个属性和服务。它接受一个MasterContext对象作为参数，包含主节点的配置信息和上下文信息。
        super.init(0, context.getConfiguration().getMasterId(), context.getConfiguration());//在方法内部，首先调用了父类（AbstractComponent）的init方法来初始化通用属性。
        this.clusterManager = context.getClusterManager();//接着通过context对象获取配置信息和其他上下文信息。
        this.resourceManager = ResourceManagerFactory.build(context);//初始化了集群管理器（clusterManager）和资源管理器（resourceManager）。


        startRpcService();//调用了startRpcService方法，启动RPC服务。

        this.masterAddress = new RpcAddress(ProcessUtil.getHostIp(), rpcPort);//创建了主节点的RPC地址（masterAddress）
        this.heartbeatManager = new HeartbeatManager(configuration, clusterManager);//初始化了心跳管理器（heartbeatManager）。
        registerHAService();//调用registerHAService方法，向高可用服务注册主节点的信息。

        ClusterContext clusterContext = new ClusterContext(configuration);//创建了集群上下文（clusterContext）对象，用于传递给集群管理器
        clusterContext.setHeartbeatManager(heartbeatManager);
        this.clusterManager.init(clusterContext);
        this.resourceManager.init(ResourceManagerContext.build(context, clusterContext));//初始化资源管理器
        this.masterAddress = new RpcAddress(ProcessUtil.getHostIp(), rpcPort);

        if (!context.getConfiguration().getBoolean(ExecutionConfigKeys.RUN_LOCAL_MODE)) {
            httpServer = new HttpServer(configuration, clusterManager, heartbeatManager);
            httpServer.start();
        }//最后，根据配置信息判断是否在本地模式下运行，如果不是，则创建并启动HTTP服务器（httpServer）以处理HTTP请求
    }

    @Override
    protected void startRpcService() {
        this.rpcService = new RpcServiceImpl(rpcPort, configuration);
        this.rpcService.addEndpoint(new MasterEndpoint(this, clusterManager));
        this.rpcService.addEndpoint(new ResourceManagerEndpoint(resourceManager));
        this.rpcPort = rpcService.startService();
    }
    //该方法用于启动RPC服务，具体实现在AbstractComponent类中。
    //首先创建了RpcServiceImpl对象，并通过调用addEndpoint方法添加了主节点和资源管理器的终端（MasterEndpoint和ResourceManagerEndpoint）。
    //最后，调用了startService方法启动RPC服务，并将rpcPort设置为返回的端口号

    public ClusterInfo startCluster() {
        RpcAddress driverAddress = clusterManager.startDriver();
        ClusterInfo clusterInfo = new ClusterInfo();
        clusterInfo.setMasterAddress(masterAddress);
        clusterInfo.setDriverAddress(driverAddress);
        LOGGER.info("init with info: {}", clusterInfo);
        return clusterInfo;
    }
    //该方法用于启动主节点集群，并返回集群信息。
    //首先通过clusterManager的startDriver方法获取驱动节点的RPC地址（driverAddress）。
    //创建了一个ClusterInfo对象，设置主节点地址和驱动节点地址。
    //返回了ClusterInfo对象，表示集群信息。

    @Override
    public void close() {
        super.close();
        clusterManager.close();
        if (heartbeatManager != null) {
            heartbeatManager.close();
        }
        if (httpServer != null) {
            httpServer.stop();
        }
        LOGGER.info("master {} closed", name);
    }
    //该方法用于关闭主节点，释放资源。
    //首先调用了父类的close方法，释放通用资源。
    //然后调用了clusterManager的close方法，关闭集群管理器。
    //如果heartbeatManager不为空，也调用了其close方法，关闭心跳管理器。
    //如果httpServer不为空，也调用了其stop方法，停止HTTP服务器。
    //最后，输出日志表示主节点已关闭

}

