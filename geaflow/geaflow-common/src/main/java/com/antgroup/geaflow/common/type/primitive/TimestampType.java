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

import com.antgroup.geaflow.common.type.IType;
import com.antgroup.geaflow.common.type.Types;
import com.google.common.primitives.Longs;
import java.sql.Timestamp;

public class TimestampType implements IType<Timestamp> {

    public static final TimestampType INSTANCE = new TimestampType();

    @Override
    public String getName() {
        return Types.TYPE_NAME_TIMESTAMP;
    }

    @Override
    public Class<Timestamp> getTypeClass() {
        return Timestamp.class;
    }

    @Override
    public byte[] serialize(Timestamp obj) {
        return Longs.toByteArray(obj.getTime());
    }

    @Override
    public Timestamp deserialize(byte[] bytes) {
        long time = Longs.fromByteArray(bytes);
        return new Timestamp(time);
    }

    @Override
    public int compare(Timestamp x, Timestamp y) {
        return Types.compare(x, y);
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
