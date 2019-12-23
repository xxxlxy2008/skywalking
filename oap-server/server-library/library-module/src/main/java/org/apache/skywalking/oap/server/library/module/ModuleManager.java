/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.skywalking.oap.server.library.module;

import java.util.*;

/**
 * The <code>ModuleManager</code> takes charge of all {@link ModuleDefine}s in collector.
 *
 * @author wu-sheng, peng-yongsheng
 */
public class ModuleManager implements ModuleDefineHolder {

    private boolean isInPrepareStage = true;
    private final Map<String, ModuleDefine> loadedModules = new HashMap<>();

    /**
     * Init the given modules
     */
    public void init(
        ApplicationConfiguration applicationConfiguration) throws ModuleNotFoundException, ProviderNotFoundException, ServiceNotProvidedException, CycleDependencyException, ModuleConfigException, ModuleStartException {
        String[] moduleNames = applicationConfiguration.moduleList(); // 根据配置拿到所有 Module的名称
        // 通过 SPI方式加载 ModuleDefine接口和 ModuleProvider接口的实现，很明显，又是微内核+插件的架构
        ServiceLoader<ModuleDefine> moduleServiceLoader = ServiceLoader.load(ModuleDefine.class);
        ServiceLoader<ModuleProvider> moduleProviderLoader = ServiceLoader.load(ModuleProvider.class);

        LinkedList<String> moduleList = new LinkedList<>(Arrays.asList(moduleNames));
        for (ModuleDefine module : moduleServiceLoader) {
            for (String moduleName : moduleNames) {
                if (moduleName.equals(module.name())) {
                    // 实例化并调用prepare()方法进行初始化
                    ModuleDefine newInstance;
                    try {
                        newInstance = module.getClass().newInstance();
                    } catch (InstantiationException | IllegalAccessException e) {
                        throw new ModuleNotFoundException(e);
                    }
                    newInstance.prepare(this, applicationConfiguration.getModuleConfiguration(moduleName), moduleProviderLoader);
                    // 记录初始化的 ModuleDefine
                    loadedModules.put(moduleName, newInstance);
                    moduleList.remove(moduleName);
                }
            }
        }
        // Finish prepare stage
        isInPrepareStage = false;

        if (moduleList.size() > 0) {
            throw new ModuleNotFoundException(moduleList.toString() + " missing.");
        }
        // 初始化 BootstrapFlow，具体的初始化逻辑后面展开分析
        BootstrapFlow bootstrapFlow = new BootstrapFlow(loadedModules);
        // 启动 Module
        bootstrapFlow.start(this);
        // 启动流程结束之后，会通过 ModuleProvider
        bootstrapFlow.notifyAfterCompleted();
    }

    @Override public boolean has(String moduleName) {
        return loadedModules.get(moduleName) != null;
    }

    @Override public ModuleProviderHolder find(String moduleName) throws ModuleNotFoundRuntimeException {
        assertPreparedStage();
        ModuleDefine module = loadedModules.get(moduleName);
        if (module != null)
            return module;
        throw new ModuleNotFoundRuntimeException(moduleName + " missing.");
    }

    private void assertPreparedStage() {
        if (isInPrepareStage) {
            throw new AssertionError("Still in preparing stage.");
        }
    }
}
