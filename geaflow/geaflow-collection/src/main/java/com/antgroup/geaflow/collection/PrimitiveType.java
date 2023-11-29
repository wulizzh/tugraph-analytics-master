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

package com.antgroup.geaflow.collection;

public enum PrimitiveType {
    INT,
    LONG,
    DOUBLE,
    BYTE,
    SHORT,
    BOOLEAN,
    FLOAT,
    BYTE_ARRAY,
    OBJECT;

    private static final String INTEGER = "INTEGER";
    private static final String BYTES = "BYTE[]";

    public static PrimitiveType getEnum(String value) {
        String up = value.toUpperCase();
        for (PrimitiveType v : values()) {
            if (v.name().equals(up)) {
                return v;
            }
        }
        if (INTEGER.equals(up)) {
            return INT;
        } else if (BYTES.equals(up)) {
            return BYTE_ARRAY;
        } else {
            return OBJECT;
        }
    }
}
