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

package com.antgroup.geaflow.common.type.primitive;

import com.antgroup.geaflow.common.binary.BinaryString;
import com.antgroup.geaflow.common.type.IType;
import com.antgroup.geaflow.common.type.Types;

public class BinaryStringType implements IType<BinaryString> {

    public static final BinaryStringType INSTANCE = new BinaryStringType();

    @Override
    public String getName() {
        return Types.TYPE_NAME_BINARY_STRING;
    }

    @Override
    public Class<BinaryString> getTypeClass() {
        return BinaryString.class;
    }

    @Override
    public byte[] serialize(BinaryString obj) {
        return obj.getBytes();
    }

    @Override
    public BinaryString deserialize(byte[] bytes) {
        return BinaryString.fromBytes(bytes);
    }

    @Override
    public int compare(BinaryString x, BinaryString y) {
        if (x == null) {
            return y == null ? 0 : -1;
        } else if (y == null) {
            return 1;
        } else {
            return x.compareTo(y);
        }
    }

    @Override
    public boolean isPrimitive() {
        return true;
    }

    @Override
    public String toString() {
        return getName();
    }
}
