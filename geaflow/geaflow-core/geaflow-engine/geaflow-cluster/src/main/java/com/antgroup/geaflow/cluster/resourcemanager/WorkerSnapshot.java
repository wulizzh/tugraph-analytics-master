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

package com.antgroup.geaflow.cluster.resourcemanager;

import java.io.Serializable;
import java.util.List;

public class WorkerSnapshot implements Serializable {

    private List<WorkerInfo> availableWorkers;
    private List<ResourceSession> sessions;

    public WorkerSnapshot() {
    }

    public WorkerSnapshot(List<WorkerInfo> availableWorkers, List<ResourceSession> sessions) {
        this.availableWorkers = availableWorkers;
        this.sessions = sessions;
    }

    public List<WorkerInfo> getAvailableWorkers() {
        return this.availableWorkers;
    }

    public void setAvailableWorkers(List<WorkerInfo> availableWorkers) {
        this.availableWorkers = availableWorkers;
    }

    public List<ResourceSession> getSessions() {
        return this.sessions;
    }

    public void setSessions(List<ResourceSession> sessions) {
        this.sessions = sessions;
    }

}
