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

package com.antgroup.geaflow.dsl.rex;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;

public class PathInputRef extends RexInputRef {

    private final String label;

    public PathInputRef(String label, int index, RelDataType type) {
        super(index, type);
        this.digest = label;
        this.label = label;
    }

    public String getLabel() {
        return label;
    }

    public PathInputRef copy(int newIndex) {
        return new PathInputRef(label, newIndex, type);
    }
}
