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

package com.antgroup.geaflow.shuffle.util;

import java.io.IOException;
import java.net.SocketAddress;

/**
 * This class is an adaptation of Flink's org.apache.flink.runtime.io.network.netty.exception.TransportException.
 */
public class TransportException extends IOException {

    private static final long serialVersionUID = 3637820720589866570L;
    private final SocketAddress address;

    public TransportException(String message, SocketAddress address) {
        this(message, address, null);
    }

    public TransportException(String message, SocketAddress address, Throwable cause) {
        super(message, cause);
        this.address = address;
    }

    public SocketAddress getAddress() {
        return address;
    }

}
