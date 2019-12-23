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

package org.apache.skywalking.oap.server.core.register.service;

import com.google.gson.JsonObject;
import java.util.Objects;
import org.apache.skywalking.oap.server.core.*;
import org.apache.skywalking.oap.server.core.cache.NetworkAddressInventoryCache;
import org.apache.skywalking.oap.server.core.register.*;
import org.apache.skywalking.oap.server.core.register.worker.InventoryStreamProcessor;
import org.apache.skywalking.oap.server.library.module.ModuleDefineHolder;
import org.slf4j.*;

import static java.util.Objects.isNull;

/**
 * @author peng-yongsheng
 */
public class NetworkAddressInventoryRegister implements INetworkAddressInventoryRegister {

    private static final Logger logger = LoggerFactory.getLogger(NetworkAddressInventoryRegister.class);

    private final ModuleDefineHolder moduleDefineHolder;
    private NetworkAddressInventoryCache networkAddressInventoryCache;
    private IServiceInventoryRegister serviceInventoryRegister;
    private IServiceInstanceInventoryRegister serviceInstanceInventoryRegister;

    public NetworkAddressInventoryRegister(ModuleDefineHolder moduleDefineHolder) {
        this.moduleDefineHolder = moduleDefineHolder;
    }

    private NetworkAddressInventoryCache getNetworkAddressInventoryCache() {
        if (isNull(networkAddressInventoryCache)) {
            this.networkAddressInventoryCache = moduleDefineHolder.find(CoreModule.NAME).provider().getService(NetworkAddressInventoryCache.class);
        }
        return this.networkAddressInventoryCache;
    }

    private IServiceInventoryRegister getServiceInventoryRegister() {
        if (isNull(serviceInventoryRegister)) {
            this.serviceInventoryRegister = moduleDefineHolder.find(CoreModule.NAME).provider().getService(IServiceInventoryRegister.class);
        }
        return this.serviceInventoryRegister;
    }

    private IServiceInstanceInventoryRegister getServiceInstanceInventoryRegister() {
        if (isNull(serviceInstanceInventoryRegister)) {
            this.serviceInstanceInventoryRegister = moduleDefineHolder.find(CoreModule.NAME).provider().getService(IServiceInstanceInventoryRegister.class);
        }
        return this.serviceInstanceInventoryRegister;
    }

    @Override public int getOrCreate(String networkAddress, JsonObject properties) {
        // 通过NetworkAddressInventoryCache缓存查找该NetworkAddress对应的id
        int addressId = getNetworkAddressInventoryCache().getAddressId(networkAddress);

        if (addressId != Const.NONE) {
            // 每个 NetworkAddress 在 service_inventory 索引中也会分配一个相应的 serviceId
            // 如果查询失败，则会进行创建，ServiceInventoryRegister的逻辑不再重复
            int serviceId = getServiceInventoryRegister().getOrCreate(addressId, networkAddress, properties);

            if (serviceId != Const.NONE) {
                // 每个 NetworkAddress 在 service_instance_inventory 索引中也会分配一个相应的 serviceInstanceId
                // 如果查询失败，则会进行创建，ServiceInstanceInventoryRegister的逻辑不再重复
                int serviceInstanceId = getServiceInstanceInventoryRegister().getOrCreate(serviceId, addressId, System.currentTimeMillis());
                if (serviceInstanceId != Const.NONE) {
                    return addressId; // 当上述查询都成功之后，才会返回
                }
            }
        } else {
            NetworkAddressInventory newNetworkAddress = new NetworkAddressInventory();
            newNetworkAddress.setName(networkAddress);

            long now = System.currentTimeMillis();
            newNetworkAddress.setRegisterTime(now);
            newNetworkAddress.setHeartbeatTime(now);
            // InventoryStreamProcessor处理，为 newNetworkAddress生成对应的id
            InventoryStreamProcessor.getInstance().in(newNetworkAddress);
        }

        return Const.NONE;
    }

    @Override public int get(String networkAddress) {
        return getNetworkAddressInventoryCache().getAddressId(networkAddress);
    }

    @Override public void heartbeat(int addressId, long heartBeatTime) {
        NetworkAddressInventory networkAddress = getNetworkAddressInventoryCache().get(addressId);
        if (Objects.nonNull(networkAddress)) {
            networkAddress = networkAddress.getClone();
            networkAddress.setHeartbeatTime(heartBeatTime);

            InventoryStreamProcessor.getInstance().in(networkAddress);
        } else {
            logger.warn("Network getAddress {} heartbeat, but not found in storage.", addressId);
        }
    }

    @Override public void update(int addressId, NodeType nodeType) {
        NetworkAddressInventory networkAddress = getNetworkAddressInventoryCache().get(addressId);

        if (!this.compare(networkAddress, nodeType)) {
            NetworkAddressInventory newNetworkAddress = networkAddress.getClone();
            newNetworkAddress.setNetworkAddressNodeType(nodeType);
            newNetworkAddress.setHeartbeatTime(System.currentTimeMillis());

            InventoryStreamProcessor.getInstance().in(newNetworkAddress);
        }
    }

    private boolean compare(NetworkAddressInventory newNetworkAddress, NodeType nodeType) {
        if (Objects.nonNull(newNetworkAddress)) {
            return nodeType.equals(newNetworkAddress.getNetworkAddressNodeType());
        }
        return true;
    }
}
