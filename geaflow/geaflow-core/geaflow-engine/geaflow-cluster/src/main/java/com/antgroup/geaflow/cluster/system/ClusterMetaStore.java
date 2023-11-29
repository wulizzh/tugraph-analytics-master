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

import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.CLUSTER_ID;

import com.antgroup.geaflow.cluster.protocol.IEvent;
import com.antgroup.geaflow.cluster.resourcemanager.WorkerSnapshot;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.pipeline.Pipeline;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterMetaStore {//是一个管理集群元数据的存储库，包含了一系列的方法和属性

    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterMetaStore.class);

    private static final String CLUSTER_META_NAMESPACE_LABEL = "framework";//集群元数据的命名空间标签
    private static final String CLUSTER_NAMESPACE_PREFIX = "cluster";//集群命名空间的前缀。
    private static final String OFFSET_NAMESPACE = "offset";//偏移量（offset）的命名空间

    private static ClusterMetaStore INSTANCE;//ClusterMetaStore 的单例实例

    private final int componentId;//组件的唯一标识
    private final Configuration configuration;//配置信息

    private IClusterMetaKVStore<String, Object> backend;//用于存储集群元数据的键值存储
    private IClusterMetaKVStore<String, Object> offsetBackend;//用于存储偏移量的键值存储。

    private ClusterMetaStore(int id, Configuration configuration) {//构造函数 ClusterMetaStore 接受两个参数，组件的唯一标识 id 和配置信息 configuration，并创建 ClusterMetaStore 的实例。
        this.componentId = id;
        this.configuration = configuration;
        String clusterId = configuration.getString(CLUSTER_ID);
        String storeKey = String.format("%s/%s/%s", CLUSTER_META_NAMESPACE_LABEL, CLUSTER_NAMESPACE_PREFIX, clusterId);
        this.backend = ClusterMetaStoreFactory.create(storeKey, id, configuration);
        LOGGER.info("create ClusterMetaStore, store key {}, id {}", storeKey, id);
    }

    public static synchronized void init(int id, Configuration configuration) {//init 方法用于初始化 ClusterMetaStore 的单例实例。它接受组件的唯一标识 id 和配置信息 configuration 作为参数。
        if (INSTANCE == null) {
            INSTANCE = new ClusterMetaStore(id, configuration);
        }
    }

    public static ClusterMetaStore getInstance(int id, Configuration configuration) {//方法用于获取 ClusterMetaStore 的单例实例，如果实例不存在则创建。
        if (INSTANCE == null) {
            init(id, configuration);
        }
        return INSTANCE;
    }

    public static ClusterMetaStore getInstance() {
        return INSTANCE;
    }

    public static synchronized void close() {//方法用于关闭 ClusterMetaStore。它关闭了存储键值对的 backend 和 offsetBackend，并将单例实例置为 null
        LOGGER.info("close ClusterMetaStore");
        if (INSTANCE != null) {
            INSTANCE.backend.close();
            if (INSTANCE.offsetBackend != null) {
                INSTANCE.offsetBackend.close();
            }
            INSTANCE = null;
        }
    }

    private IClusterMetaKVStore<String, Object> getOffsetBackend() {// 方法用于获取用于存储偏移量的键值存储，如果不存在则创建。
        if (offsetBackend == null) {
            synchronized (ClusterMetaStore.class) {
                String storeKey = String.format("%s/%s", CLUSTER_META_NAMESPACE_LABEL, OFFSET_NAMESPACE);
                offsetBackend = ClusterMetaStoreFactory.create(storeKey, componentId, configuration);
                LOGGER.info("create ClusterMetaStore, store key {}, id {}", storeKey, componentId);
            }
        }
        return offsetBackend;
    }

    public ClusterMetaStore savePipeline(Pipeline pipeline) {//方法用于保存数据处理管道信息，包括 Pipeline 对象
        save(ClusterMetaKey.PIPELINE, pipeline);
        return this;
    }

    public ClusterMetaStore savePipelineTasks(List<Integer> taskIndices) {//方法用于保存数据处理管道的任务列表，以 List<Integer> 的形式。
        save(ClusterMetaKey.PIPELINE_TASKS, taskIndices);
        return this;
    }

    public void saveWindowId(Long windowId) {//方法用于保存窗口 ID，通常是一个 Long 类型的值
        IClusterMetaKVStore<String, Object> offsetBackend = getOffsetBackend();
        offsetBackend.put(ClusterMetaKey.WINDOW_ID.name(), windowId);
        offsetBackend.flush();
    }

    public ClusterMetaStore saveCycle(Object cycle) {//方法用于保存一个对象，通常表示数据处理的循环
        save(ClusterMetaKey.CYCLE, cycle);
        return this;
    }
    
    public ClusterMetaStore saveEvent(List<IEvent> event) {//方法用于保存事件列表，通常是 IEvent 对象的列表
        save(ClusterMetaKey.EVENTS, event);
        return this;
    }

    public ClusterMetaStore saveWorkers(WorkerSnapshot workers) {//方法用于保存工作节点的快照信息，通常是 WorkerSnapshot 对象
        save(ClusterMetaKey.WORKERS, workers);
        return this;
    }

    public ClusterMetaStore saveComponentIndex(String key, Set<Integer> componentIds) {//方法用于保存组件的索引信息，通过给定的 key 和 Set<Integer> 表示。
        save(key, componentIds);
        return this;
    }

    public Pipeline getPipeline() {
        return get(ClusterMetaKey.PIPELINE);
    }//方法用于获取保存的数据处理管道信息，返回一个 Pipeline 对象。

    public List<Integer> getPipelineTasks() {
        return get(ClusterMetaKey.PIPELINE_TASKS);
    }//方法用于获取保存的数据处理管道的任务列表，返回一个 List<Integer>。

    public Long getWindowId() {//方法用于获取保存的窗口 ID，通常返回一个 Long 值
        IClusterMetaKVStore<String, Object> offsetBackend = getOffsetBackend();
        return (Long) offsetBackend.get(ClusterMetaKey.WINDOW_ID.name());
    }

    public Object getCycle() {
        return get(ClusterMetaKey.CYCLE);
    }// 方法用于获取保存的数据处理循环信息，返回一个对象

    public List<IEvent> getEvents() {
        return get(ClusterMetaKey.EVENTS);
    }//方法用于获取保存的事件列表，通常返回一个 List<IEvent> 对象

    public WorkerSnapshot getWorkers() {
        return get(ClusterMetaKey.WORKERS);
    }//法用于获取保存的工作节点快照信息，通常返回一个 WorkerSnapshot 对象。

    public Set<Integer> getComponentIds(String componentIndexLabel) {
        return get(componentIndexLabel);
    }//方法用于获取保存的事件列表，通常返回一个 List<IEvent> 对象

    public void flush() {
        backend.flush();
    }//方法用于刷新存储，可能会将内存中的数据写入到持久化存储

    public void clean() {
        // TODO Clean namespace directly from backend.
    }

    private <T> void save(ClusterMetaKey key, T value) {
        backend.put(key.name(), value);
    }

    private <T> void save(String key, T value) {
        backend.put(key, value);
    }

    private <T> T get(ClusterMetaKey key) {
        return (T) backend.get(key.name());
    }

    private <T> T get(String key) {
        return (T) backend.get(key);
    }

    public enum ClusterMetaKey {

        PIPELINE,
        PIPELINE_TASKS,
        WINDOW_ID,
        CYCLE,
        EVENTS,
        WORKERS,
    }
}
