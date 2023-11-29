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

package com.antgroup.geaflow.env;

import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.RUN_LOCAL_MODE;
import static com.antgroup.geaflow.common.config.keys.FrameworkConfigKeys.SYSTEM_OFFSET_BACKEND_TYPE;
import static com.antgroup.geaflow.common.config.keys.FrameworkConfigKeys.SYSTEM_STATE_BACKEND_TYPE;

import com.antgroup.geaflow.common.errorcode.RuntimeErrors;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.env.IEnvironment.EnvType;
import com.antgroup.geaflow.env.args.EnvironmentArgumentParser;
import com.antgroup.geaflow.env.args.IEnvironmentArgsParser;
import com.antgroup.geaflow.state.StoreType;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EnvironmentFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(EnvironmentFactory.class);//创建一个日志记录器

    public static Environment onLocalEnvironment() {
        return onLocalEnvironment(new String[]{});
    }//创建本地运行环境

    public static Environment onLocalEnvironment(String[] args) {//  这是一个公共静态方法，用于创建本地运行环境，并且接受一个args参数，该参数是一个字符串数组，包含用于配置环境的参数。
        IEnvironmentArgsParser argsParser = loadEnvironmentArgsParser();
        //创建一个 IEnvironmentArgsParser 类型的变量 argsParser，该变量将用于解析传递给方法的参数 args
        Map<String, String> config = new HashMap<>(argsParser.parse(args));
        //创建一个空的 Map 对象 config，它将用于存储配置参数。
        //调用 argsParser 对象的 parse 方法，将参数 args 解析为配置参数，并将解析后的结果存储在 config 中
        config.put(RUN_LOCAL_MODE.getKey(), Boolean.TRUE.toString());
        //运行模式标记为本地模式，这是一个默认的配置参数。
        // RUN_LOCAL_MODE.getKey() 返回与本地模式相关的键名，Boolean.TRUE.toString() 返回字符串 "true"，表示本地模式已启用
        // Set default state backend type to memory on local env.
        config.put(SYSTEM_STATE_BACKEND_TYPE.getKey(), StoreType.MEMORY.name());
        //设置状态后端类型为内存。 SYSTEM_STATE_BACKEND_TYPE.getKey() 返回与状态后端类型相关的键名，StoreType.MEMORY.name() 返回内存后端的名称。
        //设置本地运行环境的默认配置参数。这一部分代码为本地运行环境设置默认的配置参数。
        // 如果用户没有提供相关配置参数，这些默认值将用于本地运行环境。
        if (!config.containsKey(SYSTEM_OFFSET_BACKEND_TYPE.getKey())) {
            config.put(SYSTEM_OFFSET_BACKEND_TYPE.getKey(), StoreType.MEMORY.name());
        }//如果用户没有提供偏移量后端类型的配置参数，那么会使用内存作为偏移量后端的默认类型。检查是否提供了偏移量后端配置参数，如果没有，将其设置为内存。
        Environment environment = (Environment) loadEnvironment(EnvType.LOCAL);//创建本地运行环境。
        // loadEnvironment 方法根据传入的 envType 参数选择正确的实现，并返回一个环境对象。
        environment.getEnvironmentContext().withConfig(config);//配置创建的环境对象 environment，将解析后的配置参数 config 应用到该环境中。
        return environment;
    }

    public static Environment onAntEnvironment() {
        return (Environment) loadEnvironment(EnvType.RAY);
    }

    public static Environment onAntEnvironment(String[] args) {
        Environment environment = (Environment) loadEnvironment(EnvType.RAY);
        IEnvironmentArgsParser argsParser = loadEnvironmentArgsParser();
        environment.getEnvironmentContext().withConfig(argsParser.parse(args));
        return environment;
    }

    public static Environment onK8SEnvironment() {
        return (Environment) loadEnvironment(EnvType.K8S);
    }

    public static Environment onK8SEnvironment(String[] args) {
        Environment environment = (Environment) loadEnvironment(EnvType.K8S);
        IEnvironmentArgsParser argsParser = loadEnvironmentArgsParser();
        environment.getEnvironmentContext().withConfig(argsParser.parse(args));
        return environment;
    }
    // 创建 Kubernetes 环境。
    private static IEnvironment loadEnvironment(EnvType envType) {//这是一个私有静态方法，用于加载与给定环境类型 envType 相关的 IEnvironment 实现。
        ServiceLoader<IEnvironment> contextLoader = ServiceLoader.load(IEnvironment.class);//创建一个 ServiceLoader 对象 contextLoader，用于加载实现 IEnvironment 接口的服务提供者
        Iterator<IEnvironment> contextIterable = contextLoader.iterator();// - 创建一个 Iterator 对象 contextIterable，用于遍历 IEnvironment 的服务提供者
        while (contextIterable.hasNext()) {// 开始一个 while 循环，该循环用于遍历所有实现 IEnvironment 接口的服务提供者。
            IEnvironment environment = contextIterable.next();//获取下一个服务提供者实例，这里是 IEnvironment 的具体实现。
            if (environment.getEnvType() == envType) {//检查该环境实现的类型是否与目标环境类型 envType 匹配。
                LOGGER.info("loaded IEnvironment implementation {}", environment);//记录日志，表示已加载特定类型的 IEnvironment 实现。
                return environment;
            }
        }
        LOGGER.error("NOT found IEnvironment implementation with type:{}", envType);
        throw new GeaflowRuntimeException(
            RuntimeErrors.INST.spiNotFoundError(IEnvironment.class.getSimpleName()));
    }

    private static IEnvironmentArgsParser loadEnvironmentArgsParser() {// 这是一个私有静态方法，用于加载 IEnvironmentArgsParser 接口的实现，从而处理环境参数
        ServiceLoader<IEnvironmentArgsParser> contextLoader = ServiceLoader.load(IEnvironmentArgsParser.class);
        // 创建一个 ServiceLoader 对象 contextLoader，用于加载实现 IEnvironmentArgsParser 接口的服务提供者。
        Iterator<IEnvironmentArgsParser> contextIterable = contextLoader.iterator();
        //创建一个 Iterator 对象 contextIterable，用于遍历 IEnvironmentArgsParser 的服务提供者。
        boolean hasNext = contextIterable.hasNext();// 检查是否存在下一个服务提供者。
        IEnvironmentArgsParser argsParser;// 创建一个 IEnvironmentArgsParser 对象 argsParser 用于存储选择的参数解析器。
        if (hasNext) {//如果有下一个服务提供者，则执行以下操作
            argsParser = contextIterable.next();// - 获取下一个服务提供者实例，这将是 IEnvironmentArgsParser 的具体实现。
        } else {//如果没有下一个服务提供者，则执行以下操作
            // Use default argument parser.
            argsParser = new EnvironmentArgumentParser();//创建一个默认的参数解析器 EnvironmentArgumentParser。
        }
        LOGGER.info("loaded IEnvironmentArgsParser implementation {}", argsParser);//- 记录日志，表示已加载参数解析器的实现
        return argsParser;//返回选定的参数解析器作为结果。如果没有可用的实现，将返回默认解析器。
    }
}
