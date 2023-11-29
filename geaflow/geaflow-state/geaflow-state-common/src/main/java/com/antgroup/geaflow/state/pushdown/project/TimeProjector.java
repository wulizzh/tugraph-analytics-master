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

package com.antgroup.geaflow.state.pushdown.project;

import com.antgroup.geaflow.model.graph.IGraphElementWithTimeField;
import com.antgroup.geaflow.model.graph.edge.IEdge;

public class TimeProjector<K, EV> implements IProjector<IEdge<K, EV>, Long> {

    @Override
    public Long project(IEdge<K, EV> value) {
        return ((IGraphElementWithTimeField)value).getTime();
    }

    @Override
    public ProjectType projectType() {
        return ProjectType.TIME;
    }
}
