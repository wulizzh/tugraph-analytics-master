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

package com.antgroup.geaflow.pipeline;

import com.antgroup.geaflow.env.Environment;
import com.antgroup.geaflow.pipeline.callback.TaskCallBack;
import com.antgroup.geaflow.pipeline.service.PipelineService;
import com.antgroup.geaflow.pipeline.task.PipelineTask;
import com.antgroup.geaflow.view.IViewDesc;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Pipeline implements Serializable {// 这个类的作用：创建和管理数据处理管道（Pipeline）

    private transient Environment environment;//一个 Environment 对象，用于管理数据处理环境
    private List<PipelineTask> pipelineTaskList;//存储管道任务的列表
    private List<TaskCallBack> pipelineTaskCallbacks;//存储任务回调的列表
    private List<PipelineService> pipelineServices;//存储管道服务的列表
    private Map<String, IViewDesc> viewDescMap;//存储视图描述对象的映射

    public Pipeline(Environment environment) {//构造函数，接受一个 Environment 对象作为参数，并初始化了各个列表和映射
        this.environment = environment;
        this.environment.addPipeline(this);
        this.viewDescMap = new HashMap<>();
        this.pipelineTaskList = new ArrayList<>();
        this.pipelineTaskCallbacks = new ArrayList<>();
        this.pipelineServices = new ArrayList<>();
    }

    public void init() {   // 似乎是初始化管道？？？？

    }
    // 以下四个方法是为了配置管道？？
    public Pipeline withView(String viewName, IViewDesc viewDesc) { //添加视图
        this.viewDescMap.put(viewName, viewDesc);
        return this;
    }

    public TaskCallBack submit(PipelineTask pipelineTask) { //提交任务
        this.pipelineTaskList.add(pipelineTask);
        TaskCallBack taskCallBack = new TaskCallBack();
        this.pipelineTaskCallbacks.add(taskCallBack);
        return taskCallBack;
    }

    public Pipeline start(PipelineService pipelineService) { //启动服务
        this.pipelineServices.add(pipelineService);
        return this;
    }

    public Pipeline schedule(PipelineTask pipelineTask) {// 调度任务
        this.pipelineTaskList.add(pipelineTask);
        return this;
    }

    public IPipelineResult execute() { //用于执行管道，它会初始化环境、提交任务并返回结果
        this.environment.init();//环境初始化
        return this.environment.submit();
    }


    public void shutdown() {
        this.environment.shutdown();
    }//用于关闭管道，它会关闭环境，清理资源。

    public List<IViewDesc> getViewDescMap() {
        return viewDescMap.values().stream().collect(Collectors.toList());
    }

    public List<PipelineTask> getPipelineTaskList() {
        return pipelineTaskList;
    }

    public List<TaskCallBack> getPipelineTaskCallbacks() {
        return pipelineTaskCallbacks;
    }

    public List<PipelineService> getPipelineServices() {
        return pipelineServices;
    }
}
//段代码似乎是一个用于创建和管理数据处理管道的类，
// 提供了配置管道、执行管道和关闭管道的功能。
// 它还支持添加视图、任务、任务回调和服务，以用于数据处理流程的管理。
// 具体的实现和功能可能需要在其他相关类中进一步详细定义
