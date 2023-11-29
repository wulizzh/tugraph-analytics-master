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

package com.antgroup.geaflow.cluster.rpc.impl;

import com.antgroup.geaflow.cluster.rpc.RpcEndpoint;
import com.antgroup.geaflow.cluster.rpc.RpcEndpointRefFactory;
import com.antgroup.geaflow.cluster.rpc.RpcService;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.Serializable;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RpcServiceImpl implements RpcService, Serializable {//实现了RpcService接口，该接口用于提供RPC服务的实现

    private static final Logger LOGGER = LoggerFactory.getLogger(RpcServiceImpl.class);

    private int port;//RPC服务的端口号
    private Server server;//gRPC服务器对象
    private final ServerBuilder serverBuilder;//gRPC服务器构建器对象

    public RpcServiceImpl(Configuration config) {
        this(0, config);
    }

    public RpcServiceImpl(int port, Configuration config) {//它们用于初始化RPC服务
        this.serverBuilder = ServerBuilder.forPort(port);//在构造函数中，创建了serverBuilder对象，用于构建gRPC服务器
        RpcEndpointRefFactory.getInstance(config);//通过调用RpcEndpointRefFactory.getInstance(config)来初始化RPC终端引用工厂。
    }

    public void addEndpoint(RpcEndpoint rpcEndpoint) {//该方法用于向RPC服务中添加RPC终端（RpcEndpoint
        if (rpcEndpoint instanceof BindableService) {//如果rpcEndpoint是BindableService的实例，将其作为服务添加到serverBuilder中
            serverBuilder.addService((BindableService) rpcEndpoint);
        }
    }

    @Override
    public int startService() {
        try {
            this.server = serverBuilder.build().start();//首先创建了gRPC服务器对象（server）并启动
            this.port = server.getPort();
            LOGGER.info("Server started, listening on: {}", port);
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                    LOGGER.warn("*** shutting down gRPC server since JVM is shutting down");
                    stopService();
                    LOGGER.warn("*** server shut down");
                }
            });
            return port;
        } catch (Throwable t) {
            LOGGER.error(t.getMessage(), t);
            throw new GeaflowRuntimeException(t);
        }
    }
    //该方法用于启动RPC服务，并返回服务的端口号。
    //首先创建了gRPC服务器对象（server）并启动。
    //获取服务器的端口号，并将其赋给port属性。
    //输出日志表示服务器已启动，并设置了一个JVM关闭钩子，以在JVM关闭时停止服务器。
    //最后返回服务器的端口号
    @Test
    public void waitTermination() {
        try {
            server.awaitTermination();
        } catch (InterruptedException e) {
            LOGGER.warn("shutdown is interrupted");
        }
    }

    @Override
    public void stopService() {
        if (server != null) {
            server.shutdown();
        }
    }
}
