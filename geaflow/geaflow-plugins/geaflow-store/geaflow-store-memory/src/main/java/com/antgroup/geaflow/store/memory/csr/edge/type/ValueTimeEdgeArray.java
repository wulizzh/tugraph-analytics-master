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

package com.antgroup.geaflow.store.memory.csr.edge.type;

import com.antgroup.geaflow.model.graph.IGraphElementWithTimeField;
import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.model.graph.edge.impl.ValueTimeEdge;

public class ValueTimeEdgeArray<K> extends ValueEdgeArray<K> {

    private long[] times;

    public void init(Class<K> keyType, int capacity) {
        super.init(keyType, capacity);
        times = new long[capacity];
    }

    @Override
    protected IEdge<K, Object> getEdge(K sid, int pos) {
        return new ValueTimeEdge<>(sid, getDstId(pos), getValue(pos),
            getDirection(pos), times[pos]);
    }

    @Override
    public void drop() {
        super.drop();
        times = null;
    }

    @Override
    public void set(int pos, IEdge<K, Object> edge) {
        super.set(pos, edge);
        times[pos] = ((IGraphElementWithTimeField) edge).getTime();
    }

}
