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

package com.antgroup.geaflow.store.memory.csr.edge;

import com.antgroup.geaflow.model.graph.edge.IEdge;
import java.util.List;

public interface IEdgeArray<K, EV> {

    void init(Class<K> keyType, int capacity);

    void set(int pos, IEdge<K, EV> edge);

    List<IEdge<K, EV>> getRangeEdges(K sid, int start, int end);

    void drop();
}
