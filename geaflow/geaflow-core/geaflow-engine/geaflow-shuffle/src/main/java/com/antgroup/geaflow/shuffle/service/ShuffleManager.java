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

package com.antgroup.geaflow.shuffle.service;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.shuffle.api.reader.IShuffleReader;
import com.antgroup.geaflow.shuffle.api.reader.ReaderContext;
import com.antgroup.geaflow.shuffle.api.writer.IShuffleWriter;
import com.antgroup.geaflow.shuffle.config.ShuffleConfig;
import com.antgroup.geaflow.shuffle.memory.ShuffleMemoryTracker;
import com.antgroup.geaflow.shuffle.network.netty.ConnectionManager;
import com.antgroup.geaflow.shuffle.service.impl.AutoShuffleService;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShuffleManager {//用于管理Shuffle服务的类。

    private static final Logger LOGGER = LoggerFactory.getLogger(ShuffleManager.class);

    private static ShuffleManager INSTANCE;
    private final IShuffleService shuffleService;//Shuffle服务的实现，这里使用AutoShuffleService
    private final ConnectionManager connectionManager;//连接管理器，用于处理Shuffle服务的网络连接。
    private final Configuration configuration;//配置信息，包括Shuffle相关的配置。
    private IShuffleMaster shuffleMaster;

    public ShuffleManager(Configuration config) {//ShuffleManager类有一个带有config参数的构造函数，用于初始化ShuffleManager
        this.shuffleService = new AutoShuffleService();
        this.connectionManager = new ConnectionManager(ShuffleConfig.getInstance(config));
        this.shuffleService.init(connectionManager);
        this.configuration = config;
    }
    //在构造函数中，创建AutoShuffleService实例并将其分配给shuffleService。
    //初始化connectionManager，并传递Shuffle配置信息（ShuffleConfig）。
    //存储传递的配置信息到configuration成员变量。
    public static synchronized ShuffleManager init(Configuration config) {
        if (INSTANCE == null) {
            INSTANCE = new ShuffleManager(config);
            ShuffleMemoryTracker.getInstance(config);
        }
        return INSTANCE;
    }
    //该方法用于初始化ShuffleManager。
    //在方法内部，如果INSTANCE为空，创建新的ShuffleManager实例，并初始化ShuffleMemoryTracker

    public static ShuffleManager getInstance() {
        return INSTANCE;
    }

    public synchronized void initShuffleMaster() {
        if (shuffleMaster == null) {
            this.shuffleMaster = ShuffleMasterFactory.getShuffleMaster(this.configuration);
        }
    }//该方法用于初始化ShuffleMaster，用于处理Shuffle服务的主节点。

    public int getShufflePort() {
        return connectionManager.getShuffleAddress().port();
    }//该方法用于获取Shuffle服务的端口号

    public IShuffleReader loadShuffleReader(Configuration config) {
        IShuffleReader reader = shuffleService.getReader();
        reader.init(new ReaderContext(config));
        return reader;
    }

    public IShuffleWriter loadShuffleWriter() {
        return shuffleService.getWriter();
    }//该方法用于加载Shuffle Reader，负责读取Shuffle数据。
    //获取shuffleService中的Reader实例，初始化ReaderContext并返回Reade

    public synchronized IShuffleMaster getShuffleMaster() {
        return shuffleMaster;
    }

    public void close() {
        LOGGER.info("closing shuffle manager");
        shuffleMaster.close();
        try {
            connectionManager.close();
        } catch (IOException e) {
            LOGGER.warn("close connectManager failed:{}", e.getCause(), e);
        }
    }
}
