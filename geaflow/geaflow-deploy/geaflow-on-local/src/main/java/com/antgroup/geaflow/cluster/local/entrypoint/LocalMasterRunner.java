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

package com.antgroup.geaflow.cluster.local.entrypoint;

import com.antgroup.geaflow.cluster.clustermanager.ClusterInfo;
import com.antgroup.geaflow.cluster.local.clustermanager.LocalClusterManager;
import com.antgroup.geaflow.cluster.master.Master;
import com.antgroup.geaflow.cluster.master.MasterContext;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.env.IEnvironment.EnvType;

public class LocalMasterRunner {//这个类的主要作用是初始化GeaFlow主节点，配置集群管理器和环境类型，并启动主节点。

    private final Master master;//表示GeaFlow的主节点

    public LocalMasterRunner(Configuration configuration) {//构造函数LocalMasterRunner接受一个Configuration对象作为参数
        master = new Master();//创建了一个Master对象，并初始化了MasterContext对象
        MasterContext context = new MasterContext(configuration);
        context.setRecover(false);//设置了不使用恢复模式
        context.setClusterManager(new LocalClusterManager());//定了使用LocalClusterManager作为集群管理器
        context.setEnvType(EnvType.LOCAL);//以及将环境类型设置为EnvType.LOCAL
        master.init(context);
    }

    public ClusterInfo init() {//init方法用于初始化GeaFlow主节点。在此方法中，
        // 调用master对象的startCluster方法启动主节点，并返回ClusterInfo对象，该对象包含了主节点的信息。
        return master.startCluster();
    }

}
