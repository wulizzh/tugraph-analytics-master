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

package com.antgroup.geaflow.console.core.model.security.resource;

import com.antgroup.geaflow.console.common.util.type.GeaflowResourceType;

public final class AllResource extends GeaflowResource {

    private static final String ALL_RESOURCE_ID = "*";

    public AllResource(GeaflowResourceType type) {
        super(type, null);
    }

    @Override
    public String getId() {
        return ALL_RESOURCE_ID;
    }

    @Override
    public boolean include(GeaflowResource other) {
        if (other == null) {
            return false;
        }

        if (this.type.equals(other.type)) {
            return true;
        }

        return include(other.parent);
    }
}
