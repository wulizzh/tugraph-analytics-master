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

public class PipelineFactory {//用于创建不同类型的管道对象

    public static Pipeline buildPipeline(Environment environment) {
        return new Pipeline(environment);
    }
    //用于创建一个标准的管道对象，接受一个 Environment 环境对象作为参数，并返回一个新的 Pipeline 对象
    public static SchedulerPipeline buildSchedulerPipeline(Environment environment) {
        return new SchedulerPipeline(environment);//法用于创建一个调度器管道对象，也接受一个 Environment 环境对象作为参数，并返回一个新的 SchedulerPipeline 对象。
    }

}
