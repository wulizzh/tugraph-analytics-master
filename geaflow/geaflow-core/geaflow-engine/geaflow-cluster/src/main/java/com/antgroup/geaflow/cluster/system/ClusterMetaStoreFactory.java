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

package com.antgroup.geaflow.cluster.system;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.FrameworkConfigKeys;
import com.antgroup.geaflow.state.StoreType;
import com.antgroup.geaflow.state.serializer.DefaultKVSerializer;
import com.antgroup.geaflow.store.context.StoreContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterMetaStoreFactory {//作用是根据配置信息创建相应的集群元数据存储对象，以便在GeaFlow中存储集群的元数据信息。不同的存储后端可以根据需要进行切换。

    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterMetaStoreFactory.class);

    private static final int DEFAULT_SHARD_ID = 0;

    public static <K, V> IClusterMetaKVStore<K, V> create(String name, Configuration configuration) {
        return create(name, DEFAULT_SHARD_ID, configuration);
    }

    public static <K, V> IClusterMetaKVStore<K, V> create(String name, int shardId, Configuration configuration) {//方法用于创建一个IClusterMetaKVStore对象。这是一个泛型方法，它接受两参数：name（名称）、configuration（配置）。
        // 该方法的默认分片ID为0，如果需要指定不同的分片ID，可以使用另一个重载方法
        StoreContext storeContext = new StoreContext(name);//首先创建了一个StoreContext对象，用于存储存储相关的上下文信息。
        //通过storeContext设置了键值序列化器（DefaultKVSerializer）、配置信息和分片ID。
        storeContext.withKeySerializer(new DefaultKVSerializer(null, null));
        storeContext.withConfig(configuration);
        storeContext.withShardId(shardId);
        //从配置信息中获取存储后端的类型（backendType），然后调用create方法创建对应类型的IClusterMetaKVStore对象。
        String backendType = configuration.getString(FrameworkConfigKeys.SYSTEM_STATE_BACKEND_TYPE);
        IClusterMetaKVStore<K, V> store = create(StoreType.getEnum(backendType));
        store.init(storeContext);
        return store;
    }

    private static <K, V> IClusterMetaKVStore<K, V> create(StoreType storeType) {
        IClusterMetaKVStore clusterMetaKVStore;
        switch (storeType) {
            case ROCKSDB:
                LOGGER.info("create rocksdb cluster metastore");
                clusterMetaKVStore = new RocksdbClusterMetaKVStore();
                break;
            default:
                LOGGER.info("create memory cluster metastore");
                clusterMetaKVStore = new MemoryClusterMetaKVStore();
        }
        return clusterMetaKVStore;
    }
}
