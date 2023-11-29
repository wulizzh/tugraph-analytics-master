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

package com.antgroup.geaflow.operator.impl.window;

import com.antgroup.geaflow.api.function.Function;
import com.antgroup.geaflow.operator.base.window.AbstractOneInputOperator;

public class UnionOperator<I> extends AbstractOneInputOperator<I, Function> {

    public UnionOperator() {
        super();
    }

    @Override
    protected void process(I value) throws Exception {
        collectValue(value);
    }

}
