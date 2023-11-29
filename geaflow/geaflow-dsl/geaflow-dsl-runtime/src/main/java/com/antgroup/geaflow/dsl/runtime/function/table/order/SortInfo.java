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

package com.antgroup.geaflow.dsl.runtime.function.table.order;

import com.google.common.collect.Lists;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class SortInfo implements Serializable {

    public List<OrderByField> orderByFields = new ArrayList<>();

    public int fetch = -1;

    public SortInfo copy(List<OrderByField> orderByFields) {
        SortInfo sortInfo = new SortInfo();
        sortInfo.orderByFields = Lists.newArrayList(orderByFields);
        sortInfo.fetch = this.fetch;
        return sortInfo;
    }
}
