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

package com.antgroup.geaflow.dsl.rel;

import java.util.List;
import java.util.Objects;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.commons.lang3.StringUtils;

public abstract class GraphScan extends AbstractRelNode {

    protected final RelOptTable table;

    protected GraphScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table) {
        super(cluster, traitSet);
        this.table = Objects.requireNonNull(table);
    }

    @Override
    public RelOptTable getTable() {
        return table;
    }

    @Override
    protected RelDataType deriveRowType() {
        return table.getRowType();
    }

    @Override public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
            .item("table", StringUtils.join(table.getQualifiedName(), "."));
    }

    public abstract GraphScan copy(RelTraitSet traitSet, RelOptTable table);

    public GraphScan copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert inputs.isEmpty();
        return copy(traitSet, table);
    }
}
