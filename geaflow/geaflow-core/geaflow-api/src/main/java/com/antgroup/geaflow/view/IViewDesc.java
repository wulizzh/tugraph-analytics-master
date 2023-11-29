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

package com.antgroup.geaflow.view;

import java.io.Serializable;
import java.util.Map;

public interface IViewDesc extends Serializable { //该接口用于描述视图的属性和元数据

    /**
     * Returns the view name.
     */
    String getName(); //返回视图的名称，通常是一个字符串

    /**
     * Returns the shard num of view.
     */
    int getShardNum();//返回视图的分片数（shard number），表示视图的数据分割成多少块。

    /**
     * Returns the data model.
     */
    DataModel getDataModel();//返回视图的数据模型，可以是 TABLE（表格数据模型）或 GRAPH（图形数据模型）

    /**
     * Returns the backend type.
     */
    BackendType getBackend();

    /**
     * Returns the view properties.
     */
    Map getViewProps();


    enum DataModel {   //返回视图的数据模型，可以是 TABLE（表格数据模型）或 GRAPH（图形数据模型）
        // Table data model.
        TABLE,
        // Graph data model.
        GRAPH,
    }

    enum BackendType {  //回视图的后端类型，表示数据存储后端的类型
        // Default view backend, current is pangu.
        Native,
        // RocksDB backend.
        RocksDB,
        // Memory backend.
        Memory,
        // Custom backend.
        Custom
        ;//这是一个枚举类型，用于表示后端类型。包括以下成员：

         // Native：默认视图后端，通常是Pangu。
        //RocksDB：RocksDB 后端。
        //Memory：内存后端。
        //Custom：自定义后端

        public static BackendType of(String type) {
            //提供了一个 of(String type) 方法，用于根据字符串类型返回相应的枚举值。
            //如果传入的字符串不匹配任何枚举值，则抛出 IllegalArgumentException 异常。
            for (BackendType value : values()) {
                if (value.name().equalsIgnoreCase(type)) {
                    return value;
                }
            }
            throw new IllegalArgumentException("Illegal backend type: " + type);
        }
    }

}
