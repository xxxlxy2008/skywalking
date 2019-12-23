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

package org.apache.skywalking.oap.server.core.remote;

import org.apache.skywalking.oap.server.core.CoreModule;
import org.apache.skywalking.oap.server.core.remote.client.*;
import org.apache.skywalking.oap.server.core.remote.data.StreamData;
import org.apache.skywalking.oap.server.core.remote.selector.*;
import org.apache.skywalking.oap.server.library.module.*;

/**
 * @author peng-yongsheng
 */
public class RemoteSenderService implements Service {

    private final ModuleManager moduleManager;
    private final HashCodeSelector hashCodeSelector;
    private final ForeverFirstSelector foreverFirstSelector;
    private final RollingSelector rollingSelector;

    public RemoteSenderService(ModuleManager moduleManager) {
        this.moduleManager = moduleManager;
        this.hashCodeSelector = new HashCodeSelector();
        this.foreverFirstSelector = new ForeverFirstSelector();
        this.rollingSelector = new RollingSelector();
    }

    public void send(int nextWorkId, StreamData streamData, Selector selector) {
        // RemoteClientManager是用来管理一个 RemoteClient集合，其具体逻辑在后面展开
        RemoteClientManager clientManager = moduleManager.find(CoreModule.NAME).provider().getService(RemoteClientManager.class);

        RemoteClient remoteClient;
        switch (selector) {
            case HashCode: // 用hash方式查找一个RemoteClient对象，然后将RegisterSource发送出去
                remoteClient = hashCodeSelector.select(clientManager.getRemoteClient(), streamData);
                remoteClient.push(nextWorkId, streamData);
                break;
            case Rolling:
                remoteClient = rollingSelector.select(clientManager.getRemoteClient(), streamData);
                remoteClient.push(nextWorkId, streamData);
                break;
            case ForeverFirst:
                remoteClient = foreverFirstSelector.select(clientManager.getRemoteClient(), streamData);
                remoteClient.push(nextWorkId, streamData);
                break;
        }
    }
}
