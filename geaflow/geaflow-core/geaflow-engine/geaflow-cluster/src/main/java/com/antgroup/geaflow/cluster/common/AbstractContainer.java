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

import com.antgroup.geaflow.cluster.exception.ExceptionClient;
import com.antgroup.geaflow.cluster.exception.ExceptionCollectService;
import com.antgroup.geaflow.cluster.heartbeat.HeartbeatClient;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.shuffle.service.ShuffleManager;

public abstract class AbstractContainer extends AbstractComponent {//用于表示容器的通用属性和行为

    protected HeartbeatClient heartbeatClient;//心跳客户端，用于与主节点通信并注册容器
    protected ExceptionCollectService exceptionCollectService;//异常收集服务，用于处理和收集异常信息。

    public AbstractContainer(int rpcPort) {//有一个带有rpcPort参数的构造函数，用于初始化容器
        super(rpcPort);
    }

    @Override
    public void init(int id, String containerNamePrefix, Configuration configuration) {//该方法用于初始化容器，包括为容器分配一个唯一的名称（name）以及执行其他必要的初始化操作
        this.name = String.format("%s%s", containerNamePrefix, id);//在方法内部，使用传递的id和容器名称前缀（containerNamePrefix）构建容器的名称。
        super.init(id, name, configuration);//调用父类的init方法来初始化通用的属性和配置信息

        startRpcService();
        ShuffleManager.init(configuration);
        ExceptionClient.init(id, masterId);
        this.heartbeatClient = new HeartbeatClient(id, name, configuration);
        this.exceptionCollectService = new ExceptionCollectService();
        //启动RPC服务，初始化ShuffleManager，初始化异常客户端，创建心跳客户端和异常收集服务。
    }

    protected void registerToMaster() {
        this.heartbeatClient.registerToMaster(masterId, buildComponentInfo());
    }
    //该方法用于向主节点注册容器，以便进行心跳通信和管理。
    //调用心跳客户端的registerToMaster方法，并传递主节点的标识（masterId）和构建的组件信息。

    protected abstract ComponentInfo buildComponentInfo();
    //该方法是一个抽象方法，需要在具体的容器子类中实现。
    //用于构建特定于容器的组件信息，包括容器的ID、名称等。
    public void close() {
        super.close();
        if (exceptionCollectService != null) {
            exceptionCollectService.shutdown();
        }
        if (heartbeatClient != null) {
            heartbeatClient.close();
        }
    }
    //该方法用于关闭容器，释放资源和停止服务。
    //调用父类的close方法来执行通用的关闭操作。
    //如果异常收集服务和心跳客户端不为空，则分别关闭它们
}
