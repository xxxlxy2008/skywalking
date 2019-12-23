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

package org.apache.skywalking.oap.server.core.storage.cache;

import java.util.List;
import org.apache.skywalking.oap.server.core.register.ServiceInventory;
import org.apache.skywalking.oap.server.core.storage.DAO;

/**
 * @author peng-yongsheng
 */
public interface IServiceInventoryCacheDAO extends DAO {
    // 根据 serviceName 查询对应的 serviceId
    int getServiceId(String serviceName);
    // 根据 addressId 查询对应的 id
    int getServiceId(int addressId);
    // 根据 serviceId 查询对应的 ServiceInventory 对象
    ServiceInventory get(int serviceId);
    // 查询最近 30分钟内更新的 ServiceInventory数据(最多返回50条)
    List<ServiceInventory> loadLastMappingUpdate();
}
