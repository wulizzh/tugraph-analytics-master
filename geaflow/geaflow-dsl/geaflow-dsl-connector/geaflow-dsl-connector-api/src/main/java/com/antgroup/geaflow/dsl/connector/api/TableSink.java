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

package com.antgroup.geaflow.dsl.connector.api;

import com.antgroup.geaflow.api.context.RuntimeContext;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.common.types.StructType;
import java.io.IOException;
import java.io.Serializable;

/**
 * Interface for table sink.
 */
public interface TableSink extends Serializable {

    /**
     * The init method for compile time.
     */
    void init(Configuration tableConf, StructType schema);

    /**
     * The init method for runtime.
     */
    void open(RuntimeContext context);

    /**
     * The write method for writing row to the table.
     */
    void write(Row row) throws IOException;

    /**
     * The finish callback for each window finished.
     */
    void finish() throws IOException;

    /**
     * The close callback for the job finish the execution.
     */
    void close();
}
