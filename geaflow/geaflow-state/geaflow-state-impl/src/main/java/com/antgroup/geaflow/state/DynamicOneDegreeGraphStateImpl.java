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

package com.antgroup.geaflow.state;

import com.antgroup.geaflow.common.errorcode.RuntimeErrors;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.state.data.DataType;
import com.antgroup.geaflow.state.data.OneDegreeGraph;
import com.antgroup.geaflow.state.query.QueryType;
import com.antgroup.geaflow.state.strategy.manager.IGraphManager;
import java.util.List;

public class DynamicOneDegreeGraphStateImpl<K, VV, EV> extends BaseDynamicQueryState<K, VV, EV, OneDegreeGraph<K, VV, EV>>
    implements DynamicOneDegreeGraphState<K, VV, EV> {

    public DynamicOneDegreeGraphStateImpl(IGraphManager<K, VV, EV> manager) {
        super(new QueryType<>(DataType.VE), manager);
    }

    @Override
    public List<Long> getAllVersions(K id) {
        throw new GeaflowRuntimeException(RuntimeErrors.INST.unsupportedError());
    }

    @Override
    public long getLatestVersion(K id) {
        throw new GeaflowRuntimeException(RuntimeErrors.INST.unsupportedError());
    }
}
