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

package org.apache.skywalking.oap.server.cluster.plugin.zookeeper;

import com.google.common.base.Strings;
import java.util.*;
import org.apache.curator.x.discovery.*;
import org.apache.skywalking.oap.server.core.cluster.*;
import org.apache.skywalking.oap.server.core.remote.client.Address;
import org.apache.skywalking.oap.server.telemetry.api.TelemetryRelatedContext;
import org.slf4j.*;

/**
 * @author peng-yongsheng
 */
public class ZookeeperCoordinator implements ClusterRegister, ClusterNodesQuery {
    private static final Logger logger = LoggerFactory.getLogger(ZookeeperCoordinator.class);

    private final ClusterModuleZookeeperConfig config;
    private final ServiceDiscovery<RemoteInstance> serviceDiscovery;
    private volatile ServiceCache<RemoteInstance> serviceCache;
    private volatile Address selfAddress;

    ZookeeperCoordinator(ClusterModuleZookeeperConfig config, ServiceDiscovery<RemoteInstance> serviceDiscovery) {
        this.config = config;
        this.serviceDiscovery = serviceDiscovery;
    }

    @Override public synchronized void registerRemote(RemoteInstance remoteInstance) throws ServiceRegisterException {
        try {
            String remoteNamePath = "remote";
            if (needUsingInternalAddr()) {
                remoteInstance = new RemoteInstance(new Address(config.getInternalComHost(), config.getInternalComPort(), true));
            }
            // 将 RemoteInstance对象转换成 ServiceInstance对象
            ServiceInstance<RemoteInstance> thisInstance = ServiceInstance.<RemoteInstance>builder()
                .name(remoteNamePath)
                .id(UUID.randomUUID().toString()) // id随机生成
                .address(remoteInstance.getAddress().getHost())
                .port(remoteInstance.getAddress().getPort())
                .payload(remoteInstance)
                .build();
            // 将 ServiceInstance写入到 Zookeeper中
            serviceDiscovery.registerService(thisInstance);
            // 创建 ServiceCache，后续读取Zk的时候用
            serviceCache = serviceDiscovery.serviceCacheBuilder()
                .name(remoteNamePath)
                .build();
            serviceCache.start(); // 启动 ServiceCache

            this.selfAddress = remoteInstance.getAddress();
            TelemetryRelatedContext.INSTANCE.setId(selfAddress.toString());
        } catch (Exception e) {
            throw new ServiceRegisterException(e.getMessage());
        }
    }

    @Override public List<RemoteInstance> queryRemoteNodes() {
        List<RemoteInstance> remoteInstanceDetails = new ArrayList<>(20);
        if (Objects.nonNull(serviceCache)) {
            // 从 ServiceCache中查询全部的 ServiceInstance
            List<ServiceInstance<RemoteInstance>> serviceInstances = serviceCache.getInstances();
            // 遍历全部 ServiceInstance，将表示当前节点的 ServiceInstance打上isSelf=true的标记
            serviceInstances.forEach(serviceInstance -> {
                RemoteInstance instance = serviceInstance.getPayload();
                if (instance.getAddress().equals(selfAddress)) {
                    instance.getAddress().setSelf(true);
                } else {
                    instance.getAddress().setSelf(false);
                }
                remoteInstanceDetails.add(instance);
            });
        }
        return remoteInstanceDetails;
    }

    private boolean needUsingInternalAddr() {
        return !Strings.isNullOrEmpty(config.getInternalComHost()) && config.getInternalComPort() > 0;
    }
}
