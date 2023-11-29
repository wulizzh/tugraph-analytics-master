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

package com.antgroup.geaflow.dsl.runtime.traversal;

import com.antgroup.geaflow.dsl.runtime.traversal.operator.StepEndOperator;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DagGroupBuilder {

    private static final Logger LOGGER = LoggerFactory.getLogger(DagGroupBuilder.class);

    public static final String MAIN_QUERY_NAME = "Main";

    public DagTopologyGroup buildDagGroup(StepLogicalPlanSet logicalPlanSet) {
        logicalPlanSet = logicalPlanSet.markChainable();
        assert logicalPlanSet.getMainPlan().getOperator() instanceof StepEndOperator;
        LOGGER.info("[DGB]Step logical plan description:\n{}", logicalPlanSet.getPlanSetDesc());
        DagTopology mainDag = DagTopology.build(MAIN_QUERY_NAME, logicalPlanSet.getMainPlan());
        Map<String, DagTopology> subDags = new HashMap<>();
        for (Map.Entry<String, StepLogicalPlan> entry : logicalPlanSet.getSubPlans().entrySet()) {
            String queryName = entry.getKey();
            StepLogicalPlan subPlan = entry.getValue();
            DagTopology subDag = DagTopology.build(queryName, subPlan);
            subDags.put(queryName, subDag);
        }
        return new DagTopologyGroup(mainDag, subDags);
    }

    public ExecuteDagGroup buildExecuteDagGroup(StepLogicalPlanSet logicalPlanSet) {
        DagTopologyGroup dagGroup = buildDagGroup(logicalPlanSet);
        return new ExecuteDagGroupImpl(dagGroup);
    }
}
