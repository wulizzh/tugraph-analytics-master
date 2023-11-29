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

package com.antgroup.geaflow.dsl.runtime.util;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.dsl.common.exception.GeaFlowDSLException;
import com.antgroup.geaflow.dsl.parser.GeaFlowDSLParser;
import com.antgroup.geaflow.dsl.planner.GQLContext;
import com.antgroup.geaflow.dsl.schema.GeaFlowGraph;
import com.antgroup.geaflow.dsl.schema.GeaFlowTable;
import com.antgroup.geaflow.dsl.sqlnode.SqlCreateGraph;
import com.antgroup.geaflow.dsl.sqlnode.SqlCreateTable;
import com.antgroup.geaflow.dsl.sqlnode.SqlEdgeUsing;
import com.antgroup.geaflow.dsl.sqlnode.SqlVertexUsing;
import com.antgroup.geaflow.dsl.util.GQLNodeUtil;
import com.antgroup.geaflow.dsl.util.StringLiteralUtil;
import com.antgroup.geaflow.view.graph.GraphViewDesc;
import com.antgroup.geaflow.view.meta.ViewMetaBookKeeper;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSetOption;
import org.apache.calcite.sql.parser.SqlParseException;

public class QueryUtil {

    public static PreCompileResult preCompile(String script, Configuration config) {//方法用于解析查询脚本并执行预编译操作，它接受两个参数，一个是脚本字符串script，另一个是配置对象config。
        GeaFlowDSLParser parser = new GeaFlowDSLParser();//创建GeaFlowDSLParser对象，用于解析DSL语句。
        GQLContext gqlContext = GQLContext.create(config, false);//创建GQLContext对象，GQLContext用于维护查询上下文信息。
        PreCompileResult preCompileResult = new PreCompileResult();//创建PreCompileResult对象，用于存储预编译的结果。
        try {
            List<SqlNode> sqlNodes = parser.parseMultiStatement(script);//方法解析输入的脚本字符串，该方法返回一个包含多个SqlNode的列表，每个SqlNode代表一个SQL语句或命令
            List<GeaFlowTable> createTablesInScript = new ArrayList<>();//创建一个空列表createTablesInScript，用于存储在脚本中创建的数据表信息。
            Map<String, SqlCreateGraph> createGraphs = new HashMap<>();//创建一个空的Map<String, SqlCreateGraph>对象createGraphs，用于存储在脚本中创建的图表信息
            for (SqlNode sqlNode : sqlNodes) {//遍历解析后的sqlNodes列表
                if (sqlNode instanceof SqlSetOption) {//如果sqlNode是SqlSetOption类型，表示为设置选项，从中提取键值对并将其存储到config中。
                    SqlSetOption sqlSetOption = (SqlSetOption) sqlNode;
                    String key = StringLiteralUtil.toJavaString(sqlSetOption.getName());
                    String value = StringLiteralUtil.toJavaString(sqlSetOption.getValue());
                    config.put(key, value);
                } else if (sqlNode instanceof SqlCreateTable) {//如果sqlNode是SqlCreateTable类型，表示为创建数据表，将其转换为GeaFlowTable对象，并添加到createTablesInScript列表中。
                    createTablesInScript.add(gqlContext.convertToTable((SqlCreateTable) sqlNode));
                } else if (sqlNode instanceof SqlCreateGraph) {//如果sqlNode是SqlCreateGraph类型，表示为创建图表，检查其是否包含图节点和边的定义。如果包含节点或边的定义，将其转换为GeaFlowGraph对象，并通过全局配置创建该图。如果图不存在，则将其添加到insertGraphs列表中
                    SqlCreateGraph createGraph = (SqlCreateGraph) sqlNode;
                    SqlIdentifier graphName = gqlContext.completeCatalogObjName(createGraph.getName());
                    if (createGraph.getVertices().getList().stream().anyMatch(node -> node instanceof SqlVertexUsing)
                        || createGraph.getEdges().getList().stream().anyMatch(node -> node instanceof SqlEdgeUsing)) {
                        GeaFlowGraph graph = gqlContext.convertToGraph(createGraph, createTablesInScript);
                        Configuration globalConfig = graph.getConfigWithGlobal(config);
                        if (!QueryUtil.isGraphExists(graph, globalConfig)) {
                            preCompileResult.addGraph(SchemaUtil.buildGraphViewDesc(graph, globalConfig));
                        }
                    } else {
                        createGraphs.put(graphName.toString(), createGraph);
                    }
                } else if (sqlNode instanceof SqlInsert) {//如果sqlNode是SqlInsert类型，表示为插入数据到某个表，检查对应的图表是否存在，如果存在，则将其添加到insertGraphs列表中。
                    SqlInsert insert = (SqlInsert) sqlNode;
                    SqlIdentifier insertName = gqlContext.completeCatalogObjName(
                        (SqlIdentifier) insert.getTargetTable());
                    SqlIdentifier insertGraphName = GQLNodeUtil.getGraphTableName(insertName);
                    if (createGraphs.containsKey(insertGraphName.toString())) {
                        SqlCreateGraph createGraph = createGraphs.get(insertGraphName.toString());
                        GeaFlowGraph graph = gqlContext.convertToGraph(createGraph);
                        preCompileResult.addGraph(SchemaUtil.buildGraphViewDesc(graph, config));
                    } else {
                        Table graph = gqlContext.getCatalog().getGraph(gqlContext.getCurrentInstance(),
                            insertGraphName.toString());
                        if (graph != null) {
                            GeaFlowGraph geaFlowGraph = (GeaFlowGraph) graph;
                            preCompileResult.addGraph(SchemaUtil.buildGraphViewDesc(geaFlowGraph, config));
                        }
                    }
                }
            }
            return preCompileResult;//最后，返回preCompileResult对象，其中包含了通过脚本中的SQL语句创建的图表信息。
        } catch (SqlParseException e) {
            throw new GeaFlowDSLException(e);
        }
    }//这个方法的主要作用是解析DSL查询脚本，提取其中的表格和图表定义，并执行预编译操作
    // ，将相关信息存储在PreCompileResult对象中。这些信息可以用于后续的查询执行。

    public static boolean isGraphExists(GeaFlowGraph graph, Configuration globalConfig) {
        boolean graphExists;
        try {
            ViewMetaBookKeeper keeper = new ViewMetaBookKeeper(graph.getUniqueName(), globalConfig);
            long lastCheckPointId = keeper.getLatestViewVersion(graph.getUniqueName());
            graphExists = lastCheckPointId >= 0;
        } catch (IOException e) {
            throw new GeaFlowDSLException(e);
        }
        return graphExists;
    }

    public static class PreCompileResult implements Serializable {

        private final List<GraphViewDesc> insertGraphs = new ArrayList<>();

        public void addGraph(GraphViewDesc graphViewDesc) {
            insertGraphs.add(graphViewDesc);
        }

        public List<GraphViewDesc> getInsertGraphs() {
            return insertGraphs;
        }
    }
}
