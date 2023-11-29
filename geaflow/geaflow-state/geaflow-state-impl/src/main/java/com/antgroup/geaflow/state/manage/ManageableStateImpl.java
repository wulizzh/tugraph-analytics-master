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

package com.antgroup.geaflow.state.manage;

import com.antgroup.geaflow.state.strategy.manager.IStateManager;

public class ManageableStateImpl implements ManageableState {

    private IStateManager stateManager;
    private StateOperator operator;

    public ManageableStateImpl(IStateManager stateManager) {
        this.stateManager = stateManager;
        this.operator = new StateOperatorImpl(this.stateManager);
    }

    public StateOperator operate() {
        return operator;
    }

}
