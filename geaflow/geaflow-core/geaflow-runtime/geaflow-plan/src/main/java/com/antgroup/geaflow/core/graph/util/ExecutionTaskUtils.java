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

package com.antgroup.geaflow.core.graph.util;

import com.antgroup.geaflow.core.graph.ExecutionTask;
import com.antgroup.geaflow.core.graph.ExecutionTaskType;

public class ExecutionTaskUtils {

    /**
     * Check whether an execution task is the cycle head.
     */
    public static boolean isCycleHead(ExecutionTask task) {
        return task.getExecutionTaskType() == ExecutionTaskType.head
            || task.getExecutionTaskType() == ExecutionTaskType.singularity;
    }

    /**
     * Check whether an execution task is the cycle tail.
     */
    public static boolean isCycleTail(ExecutionTask task) {
        return task.getExecutionTaskType() == ExecutionTaskType.tail
            || task.getExecutionTaskType() == ExecutionTaskType.singularity;
    }
}
