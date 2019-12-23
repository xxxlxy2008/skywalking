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
import org.apache.skywalking.oap.server.library.util.CollectionUtils;
import org.slf4j.*;

/**
 * @author wu-sheng, peng-yongsheng
 */
class BootstrapFlow {
    private static final Logger logger = LoggerFactory.getLogger(BootstrapFlow.class);

    private Map<String, ModuleDefine> loadedModules;
    private List<ModuleProvider> startupSequence;

    BootstrapFlow(Map<String, ModuleDefine> loadedModules) throws CycleDependencyException {
        this.loadedModules = loadedModules;
        startupSequence = new LinkedList<>();

        makeSequence();
    }

    @SuppressWarnings("unchecked")
    void start(
        ModuleManager moduleManager) throws ModuleNotFoundException, ServiceNotProvidedException, ModuleStartException {
        for (ModuleProvider provider : startupSequence) { // 按照startupSequence集合的顺序启动
            // 检测当前 ModuleProvider依赖的 Module对象是否存在(略)
            String[] requiredModules = provider.requiredModules();
            if (requiredModules != null) {
                for (String module : requiredModules) {
                    if (!moduleManager.has(module)) {
                        throw new ModuleNotFoundException(module + " is required by " + provider.getModuleName()
                            + "." + provider.name() + ", but not found.");
                    }
                }
            }
            logger.info("start the provider {} in {} module.", provider.name(), provider.getModuleName());
            // 检查当前 ModuleProvider对象是否能提供 Module指定的服务
            provider.requiredCheck(provider.getModule().services());
            // 启动 ModuleProvider
            provider.start();
        }
    }

    void notifyAfterCompleted() throws ServiceNotProvidedException, ModuleStartException {
        for (ModuleProvider provider : startupSequence) {
            provider.notifyAfterCompleted();
        }
    }

    private void makeSequence() throws CycleDependencyException {
        // 获取全部ModuleProvider对象
        List<ModuleProvider> allProviders = new ArrayList<>();
        loadedModules.forEach((moduleName, module) -> allProviders.addAll(module.providers()));

        do {
            int numOfToBeSequenced = allProviders.size();
            for (int i = 0; i < allProviders.size(); i++) { // 尝试逐个初始化ModuleProvider对象
                ModuleProvider provider = allProviders.get(i);
                // 获取当前ModuleProvider依赖的Module
                String[] requiredModules = provider.requiredModules();
                if (CollectionUtils.isNotEmpty(requiredModules)) {
                    boolean isAllRequiredModuleStarted = true;
                    for (String module : requiredModules) {
                        // find module in all ready existed startupSequence
                        boolean exist = false;
                        // 检测依赖的Module是否已经被初始化了
                        for (ModuleProvider moduleProvider : startupSequence) {
                            if (moduleProvider.getModuleName().equals(module)) {
                                exist = true;
                                break;
                            }
                        }
                        if (!exist) {
                            isAllRequiredModuleStarted = false;
                            break;
                        }
                    }

                    if (isAllRequiredModuleStarted) {
                        startupSequence.add(provider);
                        allProviders.remove(i);
                        i--;
                    }
                } else {
                    // 添加到startupSequence集合，确定其初始化顺序
                    startupSequence.add(provider);
                    // 清理allProviders集合
                    allProviders.remove(i);
                    i--;
                }
            }
            // 检测是否出现了循环依赖(略)
            if (numOfToBeSequenced == allProviders.size()) {
                StringBuilder unSequencedProviders = new StringBuilder();
                allProviders.forEach(provider -> unSequencedProviders.append(provider.getModuleName()).append("[provider=").append(provider.getClass().getName()).append("]\n"));
                throw new CycleDependencyException("Exist cycle module dependencies in \n" + unSequencedProviders.substring(0, unSequencedProviders.length() - 1));
            }
        }
        while (allProviders.size() != 0);
    }
}
