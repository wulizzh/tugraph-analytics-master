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

import com.antgroup.geaflow.env.ctx.IEnvironmentContext;
import com.antgroup.geaflow.pipeline.IPipelineResult;
import com.antgroup.geaflow.pipeline.Pipeline;
import java.io.Serializable;

public class GeaFlowClient implements Serializable {

    private IClusterClient clusterClient;//保存集群客户端的私有变量
    private IPipelineClient pipelineClient;//保存管道客户端的私有变量

    public void init(IEnvironmentContext environmentContext, IClusterClient clusterClient) {
        this.clusterClient = clusterClient;
        this.clusterClient.init(environmentContext);
    }
    //接受环境上下文和集群客户端作为参数，将集群客户端初始化并保存在私有变量中

    public void startCluster() {//启动集群。使用已初始化的集群客户端开始集群。
        this.pipelineClient = this.clusterClient.startCluster();
    }

    public IPipelineResult submit(Pipeline pipeline) {
        return this.pipelineClient.submit(pipeline);
    }
    //submit 方法用于提交管道任务。
    //接受一个管道对象作为参数，并使用管道客户端提交该管道。

    public void shutdown() {
        this.clusterClient.shutdown();
    }

}

