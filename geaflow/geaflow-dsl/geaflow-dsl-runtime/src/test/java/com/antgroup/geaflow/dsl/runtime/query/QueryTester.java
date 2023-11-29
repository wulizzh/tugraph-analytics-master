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

package com.antgroup.geaflow.dsl.runtime.query;

import com.antgroup.geaflow.cluster.system.ClusterMetaStore;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.DSLConfigKeys;
import com.antgroup.geaflow.dsl.common.exception.GeaFlowDSLException;
import com.antgroup.geaflow.dsl.connector.file.FileConstants;
import com.antgroup.geaflow.dsl.runtime.QueryClient;
import com.antgroup.geaflow.dsl.runtime.QueryContext;
import com.antgroup.geaflow.dsl.runtime.engine.GQLPipeLine;
import com.antgroup.geaflow.dsl.runtime.engine.GQLPipeLine.GQLPipelineHook;
import com.antgroup.geaflow.env.Environment;
import com.antgroup.geaflow.env.EnvironmentFactory;
import com.antgroup.geaflow.file.FileConfigKeys;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.testng.Assert;

public class QueryTester implements Serializable {

    private int testTimeWaitSeconds = 0;//保存测试等待时间的私有变量，初始值为0。

    public static final String INIT_DDL = "/query/modern_graph.sql";
    public static final String DSL_STATE_REMOTE_PATH = "/tmp/dsl/";

    private String queryPath;

    private boolean compareWithOrder = false;

    private String graphDefinePath;

    private final Map<String, String> config = new HashMap<>();

    private QueryTester() {
        try {
            initRemotePath();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static QueryTester build() {
        return new QueryTester();
    }


    public QueryTester withQueryPath(String queryPath) {
        this.queryPath = queryPath;
        return this;
    }

    public QueryTester withTestTimeWaitSeconds(int testTimeWaitSeconds) {
        this.testTimeWaitSeconds = testTimeWaitSeconds;
        return this;
    }

    public QueryTester compareWithOrder() {
        this.compareWithOrder = true;
        return this;
    }

    public QueryTester withConfig(Map<String, String> config) {
        this.config.putAll(config);
        return this;
    }

    public QueryTester withConfig(String key, Object value) {
        this.config.put(key, String.valueOf(value));
        return this;
    }

    public QueryTester execute() throws Exception {
        if (queryPath == null) {//它检查 queryPath 是否为 null，如果是，则抛出 IllegalArgumentException异常，提示应该先调用 withQueryPath() 方法来设置查询路径。
            throw new IllegalArgumentException("You should call withQueryPath() before execute().");
        }
        Map<String, String> config = new HashMap<>();
        config.put(DSLConfigKeys.GEAFLOW_DSL_WINDOW_SIZE.getKey(), String.valueOf(-1L));
        config.put(FileConfigKeys.ROOT.getKey(), DSL_STATE_REMOTE_PATH);
        config.put(DSLConfigKeys.GEAFLOW_DSL_QUERY_PATH.getKey(), FileConstants.PREFIX_JAVA_RESOURCE + queryPath);
        config.putAll(this.config);
        //然后，它创建一个配置映射 config，并添加了一系列配置项，包括窗口大小、文件根目录和查询路径
        initResultDirectory();//方法用于初始化结果目录。

        Environment environment = EnvironmentFactory.onLocalEnvironment(new String[]{});//接着，它创建了一个本地环境对象 environment，并为该环境设置了配置参数
        environment.getEnvironmentContext().withConfig(config);

        GQLPipeLine gqlPipeLine = new GQLPipeLine(environment, testTimeWaitSeconds);
        //创建了一个 GQL 管道对象 gqlPipeLine，传入了 environment 和 testTimeWaitSeconds 参数。
        String graphDefinePath = null;
        if (this.graphDefinePath != null) {
            graphDefinePath = this.graphDefinePath;
        }//如果图定义路径 graphDefinePath 不为 null，则将其设置为 graphDefinePath。
        gqlPipeLine.setPipelineHook(new TestGQLPipelineHook(graphDefinePath, queryPath));
        //然后，通过 setPipelineHook 方法设置了管道的钩子，
        // 这个钩子是 TestGQLPipelineHook 的实例，用于在执行查询前后进行操作。
        try {
            gqlPipeLine.execute();
        } finally {
            environment.shutdown();
            ClusterMetaStore.close();
        }
        return this;
    }

    private void initResultDirectory() throws Exception {
        // delete target file path
        String targetPath = getTargetPath(queryPath);
        File targetFile = new File(targetPath);
        if (targetFile.exists()) {
            FileUtils.forceDelete(targetFile);
        }
    }

    private void initRemotePath() throws IOException {
        // delete state remote path
        File stateRemoteFile = new File(DSL_STATE_REMOTE_PATH);
        if (stateRemoteFile.exists()) {
            FileUtils.forceDelete(stateRemoteFile);
        }
    }

    public void checkSinkResult() throws Exception {
        String[] paths = queryPath.split("/");
        String lastPath = paths[paths.length - 1];
        String exceptPath = "/expect/" + lastPath.split("\\.")[0] + ".txt";
        String targetPath = getTargetPath(queryPath);
        String expectResult = IOUtils.resourceToString(exceptPath, Charset.defaultCharset()).trim();
        String actualResult = readFile(targetPath);
        compareResult(actualResult, expectResult);
    }

    private void compareResult(String actualResult, String expectResult) {
        if (compareWithOrder) {
            Assert.assertEquals(actualResult, expectResult);
        } else {
            String[] actualLines = actualResult.split("\n");
            Arrays.sort(actualLines);
            String[] expectLines = expectResult.split("\n");
            Arrays.sort(expectLines);

            String actualSort = StringUtils.join(actualLines, "\n");
            String expectSort = StringUtils.join(expectLines, "\n");
            if (!Objects.equals(actualSort, expectSort)) {
                Assert.assertEquals(actualResult, expectResult);
            }
        }
    }

    private String readFile(String path) throws IOException {
        File file = new File(path);
        if (file.isHidden()) {
            return "";
        }
        if (file.isFile()) {
            return IOUtils.toString(new File(path).toURI(), Charset.defaultCharset()).trim();
        }
        File[] files = file.listFiles();
        StringBuilder content = new StringBuilder();
        if (files != null) {
            for (File subFile : files) {
                String readText = readFile(subFile.getAbsolutePath());
                if (StringUtils.isBlank(readText)) {
                    continue;
                }
                if (content.length() > 0) {
                    content.append("\n");
                }
                content.append(readText);
            }
        }
        return content.toString().trim();
    }

    private static String getTargetPath(String queryPath) {
        assert queryPath != null;
        String[] paths = queryPath.split("/");
        String lastPath = paths[paths.length - 1];
        String targetPath = "target/" + lastPath.split("\\.")[0];
        String currentPath = new File(".").getAbsolutePath();
        targetPath = currentPath.substring(0, currentPath.length() - 1) + targetPath;
        return targetPath;
    }

    public QueryTester withGraphDefine(String graphDefinePath) {
        this.graphDefinePath = Objects.requireNonNull(graphDefinePath);
        return this;
    }

    private static class TestGQLPipelineHook implements GQLPipelineHook {

        private final String graphDefinePath;

        private final String queryPath;

        public TestGQLPipelineHook(String graphDefinePath, String queryPath) {
            this.graphDefinePath = graphDefinePath;
            this.queryPath = queryPath;
        }

        @Override
        public String rewriteScript(String script, Configuration configuration) {
            return script.replace("${target}", getTargetPath(queryPath));
        }

        @Override
        public void beforeExecute(QueryClient queryClient, QueryContext queryContext) {
            if (graphDefinePath != null) {
                try {
                    String ddl = IOUtils.resourceToString(graphDefinePath, Charset.defaultCharset());
                    queryClient.executeQuery(ddl, queryContext);
                } catch (IOException e) {
                    throw new GeaFlowDSLException(e);
                }
            }
        }

        @Override
        public void afterExecute(QueryClient queryClient, QueryContext queryContext) {

        }
    }
}
