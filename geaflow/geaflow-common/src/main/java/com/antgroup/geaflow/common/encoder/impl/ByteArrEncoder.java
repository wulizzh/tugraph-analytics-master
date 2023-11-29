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

package com.antgroup.geaflow.common.encoder.impl;

import com.antgroup.geaflow.common.encoder.Encoders;
import com.antgroup.geaflow.common.errorcode.RuntimeErrors;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class ByteArrEncoder extends AbstractEncoder<byte[]> {

    public static final ByteArrEncoder INSTANCE = new ByteArrEncoder();

    @Override
    public void encode(byte[] data, OutputStream outputStream) throws IOException {
        if (data == null) {
            Encoders.INTEGER.encode(NULL, outputStream);
            return;
        }
        int lenToWrite = data.length + 1;
        if (lenToWrite < 0) {
            throw new GeaflowRuntimeException(RuntimeErrors.INST.typeSysError(MSG_ARR_TOO_BIG));
        }
        Encoders.INTEGER.encode(lenToWrite, outputStream);
        for (byte datum : data) {
            Encoders.BYTE.encode(datum, outputStream);
        }
    }

    @Override
    public byte[] decode(InputStream inputStream) throws IOException {
        int flag = Encoders.INTEGER.decode(inputStream);
        if (flag == NULL) {
            return null;
        }
        int length = flag - 1;
        byte[] arr = new byte[length];
        for (int i = 0; i < length; i++) {
            arr[i] = Encoders.BYTE.decode(inputStream);
        }
        return arr;
    }

}
