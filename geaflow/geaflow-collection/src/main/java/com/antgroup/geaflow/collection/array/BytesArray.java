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

package com.antgroup.geaflow.collection.array;

public class BytesArray implements PrimitiveArray<byte[]> {

    private byte[][] arr;

    public BytesArray(int capacity) {
        arr = new byte[capacity][];
    }

    @Override
    public void set(int pos, byte[] value) {
        arr[pos] = value;
    }

    @Override
    public byte[] get(int pos) {
        return arr[pos];
    }

    @Override
    public void drop() {
        this.arr = null;
    }
}
