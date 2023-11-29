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

package com.antgroup.geaflow.example.stream;

import static com.antgroup.geaflow.example.config.ExampleConfigKeys.AGG_PARALLELISM;
import static com.antgroup.geaflow.example.config.ExampleConfigKeys.SINK_PARALLELISM;

import com.antgroup.geaflow.api.collector.Collector;
import com.antgroup.geaflow.api.function.base.AggregateFunction;
import com.antgroup.geaflow.api.function.base.FlatMapFunction;
import com.antgroup.geaflow.api.function.io.SinkFunction;
import com.antgroup.geaflow.api.pdata.stream.window.PWindowSource;
import com.antgroup.geaflow.api.window.impl.SizeTumblingWindow;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.tuple.Tuple;
import com.antgroup.geaflow.env.Environment;
import com.antgroup.geaflow.env.ctx.EnvironmentContext;
import com.antgroup.geaflow.example.function.FileSink;
import com.antgroup.geaflow.example.function.FileSource;
import com.antgroup.geaflow.example.util.ExampleSinkFunctionFactory;
import com.antgroup.geaflow.example.util.ResultValidator;
import com.antgroup.geaflow.pipeline.IPipelineResult;
import com.antgroup.geaflow.pipeline.Pipeline;
import com.antgroup.geaflow.pipeline.PipelineFactory;
import com.antgroup.geaflow.pipeline.task.IPipelineTaskContext;
import com.antgroup.geaflow.pipeline.task.PipelineTask;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.Comparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamUnionPipeline implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamUnionPipeline.class);

    public static final String RESULT_FILE_PATH = "./target/tmp/data/result/union";
    public static final String REF_FILE_PATH = "data/reference/union";
    public static final String SPLIT = ",";

    public IPipelineResult submit(Environment environment) {
        Pipeline pipeline = PipelineFactory.buildPipeline(environment);
        Configuration envConfig = ((EnvironmentContext) environment.getEnvironmentContext()).getConfig();
        envConfig.getConfigMap().put(FileSink.OUTPUT_DIR, RESULT_FILE_PATH);
        ResultValidator.cleanResult(RESULT_FILE_PATH);
        pipeline.submit(new PipelineTask() {
            @Override
            public void execute(IPipelineTaskContext pipelineTaskCxt) {
                Configuration conf = pipelineTaskCxt.getConfig();
                PWindowSource<String> streamSource =
                    pipelineTaskCxt.buildSource(new FileSource<String>("data/input"
                    + "/email_edge",
                        Collections::singletonList) {}, SizeTumblingWindow.of(5000));

                PWindowSource<String> streamSource2 =
                    pipelineTaskCxt.buildSource(new FileSource<String>("data/input"
                        + "/email_edge",
                            Collections::singletonList) {}, SizeTumblingWindow.of(5000));

                SinkFunction<String> sink = ExampleSinkFunctionFactory.getSinkFunction(conf);
                streamSource
                    .union(streamSource2)
                    .flatMap(new FlatMapFunction<String, Long>() {
                        @Override
                        public void flatMap(String value, Collector collector) {
                            String[] records = value.split(SPLIT);
                            for (String record : records) {
                                collector.partition(Long.valueOf(record));
                            }
                        }
                    })
                    .map(p -> Tuple.of(p, p))
                    .keyBy(p -> p)
                    .materialize()
                    .aggregate(new AggFunc())
                    .withParallelism(conf.getInteger(AGG_PARALLELISM))
                    .map(v -> String.format("%s", v))
                    .sink(sink)
                    .withParallelism(conf.getInteger(SINK_PARALLELISM));
            }
        });

        return pipeline.execute();
    }

    public static void validateResult() throws IOException {
        ResultValidator.validateMapResult(REF_FILE_PATH, RESULT_FILE_PATH,
            Comparator.comparingLong(StreamUnionPipeline::parseSumValue));
    }

    private static long parseSumValue(String result) {
        String sumValue = result.split(",")[1];
        sumValue = sumValue.substring(0, sumValue.length() - 1);
        return Long.parseLong(sumValue);
    }

    public static class AggFunc implements
            AggregateFunction<Tuple<Long, Long>, Tuple<Long, Long>, Tuple<Long, Long>> {

        @Override
        public Tuple<Long, Long> createAccumulator() {
            return Tuple.of(0L, 0L);
        }

        @Override
        public void add(Tuple<Long, Long> value, Tuple<Long, Long> accumulator) {
            accumulator.setF0(value.f0);
            accumulator.setF1(value.f1 + accumulator.f1);
        }

        @Override
        public Tuple<Long, Long> getResult(Tuple<Long, Long> accumulator) {
            return Tuple.of(accumulator.f0, accumulator.f1);
        }

        @Override
        public Tuple<Long, Long> merge(Tuple<Long, Long> a, Tuple<Long, Long> b) {
            return null;
        }
    }
}
