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

package com.antgroup.geaflow.cluster.client;

import com.antgroup.geaflow.env.Environment;
import com.antgroup.geaflow.pipeline.IPipelineResult;

public abstract class AbstractEnvironment extends Environment {

    protected GeaFlowClient geaflowClient;

    @Override
    public void init() {//用于初始化环境
        IClusterClient clusterClient = getClusterClient();//getClusterClient方法获取集群客户端对象。
        this.geaflowClient = new GeaFlowClient();//然后，创建一个GeaFlowClient对象并初始化它
        this.geaflowClient.init(context, clusterClient);//，传递了当前环境的上下文信息和集群客户端对象
        this.geaflowClient.startCluster();//调用geaflowClient的startCluster方法启动集群。
    }

    @Override
    public IPipelineResult submit() {
        return this.geaflowClient.submit(this.pipeline);
    }

    @Override
    public void shutdown() {
        if (this.geaflowClient != null) {
            this.geaflowClient.shutdown();
        }
    }

    protected abstract IClusterClient getClusterClient();

}
