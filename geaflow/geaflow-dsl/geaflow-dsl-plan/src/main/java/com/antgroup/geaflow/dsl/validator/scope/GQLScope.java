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

package com.antgroup.geaflow.dsl.validator.scope;

import com.antgroup.geaflow.dsl.validator.GQLValidatorImpl;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.ListScope;
import org.apache.calcite.sql.validate.SqlValidatorScope;

public class GQLScope extends ListScope {

    protected final SqlNode node;

    public GQLScope(SqlValidatorScope parent, SqlNode node) {
        super(parent);
        this.node = node;
    }

    @Override
    public SqlNode getNode() {
        return node;
    }

    @Override
    public SqlValidatorScope getOperandScope(SqlCall call) {
        SqlValidatorScope scope = ((GQLValidatorImpl) validator).getScopes(call);
        if (scope != null) {
            return scope;
        }
        return super.getOperandScope(call);
    }
}
