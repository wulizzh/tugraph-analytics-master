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

package com.antgroup.geaflow.cluster.local.client;

import com.antgroup.geaflow.cluster.client.AbstractClusterClient;
import com.antgroup.geaflow.cluster.client.IPipelineClient;
import com.antgroup.geaflow.cluster.client.PipelineClient;
import com.antgroup.geaflow.cluster.client.callback.ClusterStartedCallback.ClusterMeta;
import com.antgroup.geaflow.cluster.clustermanager.ClusterContext;
import com.antgroup.geaflow.cluster.clustermanager.ClusterInfo;
import com.antgroup.geaflow.cluster.local.clustermanager.LocalClient;
import com.antgroup.geaflow.cluster.local.clustermanager.LocalClusterId;
import com.antgroup.geaflow.cluster.local.clustermanager.LocalClusterManager;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.env.ctx.IEnvironmentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalClusterClient extends AbstractClusterClient {//表示它是一个本地集群客户端。

    private static final Logger LOGGER = LoggerFactory.getLogger(LocalClusterClient.class);
    private LocalClusterManager localClusterManager;//用于管理本地集群
    private ClusterContext clusterContext;//用于存储集群上下文信息。

    @Override
    public void init(IEnvironmentContext environmentContext) {//init方法用于初始化本地集群客户端。它接受IEnvironmentContext对象作为参数，
        super.init(environmentContext);//首先调用父类的init方法来初始化环境上下文。
        clusterContext = new ClusterContext(config);//然后，它创建一个ClusterContext对象，并将其初始化为clusterContext。
        localClusterManager = new LocalClusterManager();//接着，它创建一个LocalClusterManager对象并初始化它，
        // 将其设置为localClusterManager。
        localClusterManager.init(clusterContext);
    }

    @Override
    public IPipelineClient startCluster() {//方法用于启动本地集群
        try {
            LocalClusterId clusterId = localClusterManager.startMaster();//首先尝试通过localClusterManager启动主节点
            ClusterInfo clusterInfo = LocalClient.initMaster(clusterId.getMaster());//然后通过LocalClient初始化主节点的集群信息
            ClusterMeta clusterMeta = new ClusterMeta(clusterInfo);//接着，它创建一个ClusterMeta对象表示集群的元数据
            callback.onSuccess(clusterMeta);//并通过callback对象的onSuccess方法通知成功启动了集群
            LOGGER.info("cluster info: {}", clusterInfo);
            return new PipelineClient(clusterInfo.getDriverAddress(), clusterContext.getConfig());//最后，返回一个PipelineClient对象，用于与集群的驱动器进行通信。
        } catch (Throwable e) {
            LOGGER.error("deploy cluster failed", e);
            callback.onFailure(e);
            throw new GeaflowRuntimeException(e);
        }
    }

    @Override
    public void shutdown() {//shutdown方法用于关闭本地集群。它首先记录一条日志，然后调用localClusterManager的close方法来关闭集群
        LOGGER.info("shutdown cluster");
        localClusterManager.close();
    }

}
