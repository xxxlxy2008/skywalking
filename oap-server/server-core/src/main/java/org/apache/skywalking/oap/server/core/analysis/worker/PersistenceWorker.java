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

package org.apache.skywalking.oap.server.core.analysis.worker;

import java.util.*;
import org.apache.skywalking.oap.server.core.analysis.data.Window;
import org.apache.skywalking.oap.server.core.storage.*;
import org.apache.skywalking.oap.server.core.worker.AbstractWorker;
import org.apache.skywalking.oap.server.library.module.ModuleDefineHolder;
import org.slf4j.*;

/**
 * @author peng-yongsheng
 */
public abstract class PersistenceWorker<INPUT extends StorageData, CACHE extends Window<INPUT>> extends AbstractWorker<INPUT> {

    private static final Logger logger = LoggerFactory.getLogger(PersistenceWorker.class);

    private final int batchSize;
    private final IBatchDAO batchDAO;

    PersistenceWorker(ModuleDefineHolder moduleDefineHolder, int batchSize) {
        super(moduleDefineHolder);
        this.batchSize = batchSize;
        this.batchDAO = moduleDefineHolder.find(StorageModule.NAME).provider().getService(IBatchDAO.class);
    }

    void onWork(INPUT input) {
        // 缓存中累计的数据够多了，会切换一次缓冲区，并进行一次批量写操作
        if (getCache().currentCollectionSize() >= batchSize) {
            try {
                if (getCache().trySwitchPointer()) { // 检测是否符合切换缓冲队列的条件
                    getCache().switchPointer(); // 切换缓冲队列
                    // 创建一批请求并批量执行
                    List<?> collection = buildBatchCollection();
                    batchDAO.batchPersistence(collection);
                }
            } finally {
                getCache().trySwitchPointerFinally();
            }
        }
        cacheData(input); // 写入缓存
    }

    public abstract void cacheData(INPUT input);

    public abstract CACHE getCache();

    public boolean flushAndSwitch() {
        boolean isSwitch;
        try {
            if (isSwitch = getCache().trySwitchPointer()) {
                getCache().switchPointer();
            }
        } finally {
            getCache().trySwitchPointerFinally();
        }
        return isSwitch;
    }

    public abstract List<Object> prepareBatch(CACHE cache);

    public final List<?> buildBatchCollection() {
        List<?> batchCollection = new LinkedList<>();
        try {
            // 这里等待即将读取的缓冲队列停写
            while (getCache().getLast().isWriting()) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    logger.warn("thread wake up");
                }
            }
            // 获取缓冲队列
            if (getCache().getLast().collection() != null) {
                batchCollection = prepareBatch(getCache());
            }
        } finally {
            getCache().finishReadingLast();
        }
        return batchCollection;
    }
}
