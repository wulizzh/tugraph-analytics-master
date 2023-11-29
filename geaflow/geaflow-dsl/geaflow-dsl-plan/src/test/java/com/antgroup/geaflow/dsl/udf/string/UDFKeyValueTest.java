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

package com.antgroup.geaflow.dsl.udf.string;

import com.antgroup.geaflow.dsl.udf.table.string.KeyValue;
import org.testng.annotations.Test;

public class UDFKeyValueTest {

    @Test
    public void test() {

        KeyValue kv = new KeyValue();

        System.out.println(kv.eval(
            "xxx^", "^", "=", "ip"
        ));

    }
}
