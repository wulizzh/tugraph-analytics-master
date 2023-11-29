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

package com.antgroup.geaflow.cluster.task.runner;

import com.antgroup.geaflow.common.exception.GeaflowInterruptedException;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractTaskRunner<TASK> implements ITaskRunner<TASK> {//是一个抽象类，用于执行任务的运行器。

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractTaskRunner.class);
    private static final int POOL_TIMEOUT = 100;

    private final LinkedBlockingQueue<TASK> taskQueue;//taskQueue：一个LinkedBlockingQueue，用于存储任务
    protected volatile boolean running;

    public AbstractTaskRunner() {
        this.running = true;
        this.taskQueue = new LinkedBlockingQueue<>();
    }

    @Override
    public void run() {
        while (running) {
            try {
                TASK task = taskQueue.poll(POOL_TIMEOUT, TimeUnit.MILLISECONDS);
                if (running && task != null) {
                    process(task);
                }
            } catch (InterruptedException e) {
                throw new GeaflowInterruptedException(e);
            } catch (Throwable t) {
                LOGGER.error(t.getMessage(), t);
                throw new GeaflowRuntimeException(t);
            }
        }
    }
    //实现了Runnable接口的run方法，用于运行任务。
    //在一个循环中，从taskQueue中获取任务，并调用process方法处理任务。
    //如果在等待获取任务时线程被中断，会抛出GeaflowInterruptedException异常。
    //如果在处理任务时发生异常，会抛出GeaflowRuntimeException异常
    @Override
    public void add(TASK task) {
        this.taskQueue.add(task);
    }

    protected abstract void process(TASK task);

    @Override
    public void interrupt() {
        // TODO interrupt running task.
    }

    @Override
    public void shutdown() {
        this.running = false;
    }
}
