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

package com.antgroup.geaflow.store;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.state.DataModel;
import java.util.List;

/**
 * The store builder interface is built by SPI, and used to get the specific store.
 */
public interface IStoreBuilder {

    /**
     * Returns the specific store by {@link DataModel}.
     */
    IBaseStore getStore(DataModel type, Configuration config);

    /**
     * Returns the store descriptor.
     */
    StoreDesc getStoreDesc();

    /**
     * Returns the data models supported by the store builder.
     */
    List<DataModel> supportedDataModel();
}
