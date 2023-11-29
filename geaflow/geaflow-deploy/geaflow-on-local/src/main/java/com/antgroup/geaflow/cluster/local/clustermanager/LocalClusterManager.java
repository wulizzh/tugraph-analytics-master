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

package com.antgroup.geaflow.cluster.local.clustermanager;

import com.antgroup.geaflow.cluster.clustermanager.AbstractClusterManager;
import com.antgroup.geaflow.cluster.clustermanager.ClusterContext;
import com.antgroup.geaflow.cluster.container.ContainerContext;
import com.antgroup.geaflow.cluster.driver.DriverContext;
import com.antgroup.geaflow.cluster.failover.FailoverStrategyType;
import com.antgroup.geaflow.cluster.failover.FoStrategyFactory;
import com.antgroup.geaflow.cluster.failover.IFailoverStrategy;
import com.antgroup.geaflow.cluster.local.context.LocalContainerContext;
import com.antgroup.geaflow.cluster.local.context.LocalDriverContext;
import com.antgroup.geaflow.env.IEnvironment;
import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalClusterManager extends AbstractClusterManager {//LocalClusterManager类继承自AbstractClusterManager，
    // 表示它是一个本地集群管理器，用于管理本地集群中的容器和驱动器。

    private static final Logger LOGGER = LoggerFactory.getLogger(LocalClusterManager.class);
    private String appPath;//声明了一个私有成员变量appPath，用于存储应用程序路径。

    @Override
    public void init(ClusterContext clusterContext) {//init方法用于初始化本地集群管理器，
        // 它接受ClusterContext对象作为参数，并调用父类的init方法来初始化集群上下文。然后，它设置appPath为当前工作路径
        super.init(clusterContext);
        this.appPath = getWorkPath();
    }

    @Override
    protected IFailoverStrategy buildFoStrategy() {//方法用于构建故障转移策略。它使用FoStrategyFactory加载本地环境的故障转移策略，策略类型为disable_fo
        return FoStrategyFactory.loadFoStrategy(IEnvironment.EnvType.LOCAL, FailoverStrategyType.disable_fo.name());
    }

    @Override
    public LocalClusterId startMaster() {//方法用于启动主节点（Master）。首先，它检查clusterConfig是否已初始化，
        // 然后创建一个LocalClusterId对象表示本地集群，通过LocalClient创建主节点，并将其设置为clusterInfo
        Preconditions.checkArgument(clusterConfig != null, "clusterConfig is not initialized");
        clusterInfo = new LocalClusterId(LocalClient.createMaster(clusterConfig));
        return (LocalClusterId) clusterInfo;
    }

    @Override
    public void restartContainer(int containerId) {//restartContainer方法用于重新启动容器，但在本地集群管理器中不执行任何操作。
        // do nothing.
    }

    @Override
    public void doStartContainer(int containerId, boolean isRecover) {//restartContainer方法用于重新启动容器，但在本地集群管理器中不执行任何操作。
        ContainerContext containerContext = new LocalContainerContext(containerId,
            clusterConfig.getConfig());
        LocalClient.createContainer(clusterConfig, containerContext);
    }

    @Override
    public void doStartDriver(int driverId) {//法用于启动驱动器。它接受驱动器ID作为参数。在本地集群管理器中，创建一个LocalDriverContext对象，然后通过LocalClient创建驱动器。
        DriverContext driverContext = new LocalDriverContext(driverId, clusterConfig.getConfig());
        LocalClient.createDriver(clusterConfig, driverContext);
        LOGGER.info("call driver start");
    }

    @Override
    public void close() {//close方法用于关闭本地集群管理器。它首先调用父类的close方法，
        // 然后检查appPath是否已初始化。如果appPath不为空，它会删除应用程序路径下的文件
        super.close();
        if (appPath != null) {
            FileUtils.deleteQuietly(new File(appPath));
        }
    }

    private String getWorkPath() {//getWorkPath方法用于获取工作路径。
        // 它创建一个基于当前时间戳的临时工作路径，并在本地文件系统中创建该路径。
        String workPath = "/tmp/" + System.currentTimeMillis();
        try {
            FileUtils.forceMkdir(new File(workPath));
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
        }
        return workPath;
    }

}
