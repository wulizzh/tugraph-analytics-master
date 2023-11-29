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

package com.antgroup.geaflow.cluster.task;

import com.antgroup.geaflow.cluster.protocol.IExecutableCommand;
//这个类主要用于封装任务的执行和管理，它持有任务的上下文和要执行的命令，
// 并提供方法来执行、中断和关闭任务。
// 任务的具体实现和逻辑由命令对象（IExecutableCommand的实现类）负责
public class Task implements ITask {//该接口包含了处理任务的方法。

    private ITaskContext context;
    private IExecutableCommand command;//表示要执行的命令，即IExecutableCommand类型的对象。

    public Task() {
    }

    @Override
    public void init(ITaskContext taskContext) {
        this.context = taskContext;
        this.command = null;
    }
    //用于初始化任务的上下文，将传入的ITaskContext对象赋给context成员变量。
    @Override
    public void execute(IExecutableCommand command) {
        this.command = command;
        command.execute(this.context);
    }
    //用于执行任务，接受一个IExecutableCommand对象作为参数。
    //在方法中，将传入的命令对象赋给command成员变量，然后调用命令对象的execute方法，传入当前任务的上下文对象

    @Override
    public void interrupt() {
        if (command != null) {
            command.interrupt();
        }
    }
    //用于中断任务的执行，如果当前任务有关联的命令对象，就调用命令对象的interrupt方法

    @Override
    public void close() {
        this.context.close();
    }

}
