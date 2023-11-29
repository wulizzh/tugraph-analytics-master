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

package com.antgroup.geaflow.operator.impl.graph.algo.vc.msgbox;

import com.antgroup.geaflow.api.graph.function.vc.VertexCentricCombineFunction;
import com.antgroup.geaflow.collector.ICollector;
import com.antgroup.geaflow.model.graph.message.IGraphMessage;

public class GraphMsgBoxFactory {

    public static <K, MESSAGE> IGraphMsgBox<K, MESSAGE> buildMessageBox(
        ICollector<IGraphMessage<K, MESSAGE>> msgCollector,
        VertexCentricCombineFunction<MESSAGE> combineFunction) {
        if (combineFunction == null) {
            return new DirectEmitMsgBox<>(msgCollector);
        } else {
            return new CombinedMsgBox<>(combineFunction);
        }
    }

}
