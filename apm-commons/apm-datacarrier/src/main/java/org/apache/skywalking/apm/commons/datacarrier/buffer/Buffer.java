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

package org.apache.skywalking.apm.commons.datacarrier.buffer;

import java.util.*;
import org.apache.skywalking.apm.commons.datacarrier.callback.QueueBlockingCallback;
import org.apache.skywalking.apm.commons.datacarrier.common.AtomicRangeInteger;

/**
 * Created by wusheng on 2016/10/25.
 */
public class Buffer<T> {
    // 真正缓存数据的数组
    private final Object[] buffer;

    // 1.BLOCKING（默认），写入线程阻塞等待，直到数据被消费为止
    // 2.OVERRIDE，复写旧数据，旧数据被丢弃
    // 3.IF_POSSIBLE，如果无法写入则直接返回false，由应用程序判断如何处理
    private BufferStrategy strategy;

    // 环形的指针，后面展开详细说其实现
    private AtomicRangeInteger index;

    // 回调函数集合
    private List<QueueBlockingCallback<T>> callbacks;

    Buffer(int bufferSize, BufferStrategy strategy) {
        buffer = new Object[bufferSize];
        this.strategy = strategy;
        index = new AtomicRangeInteger(0, bufferSize);
        callbacks = new LinkedList<QueueBlockingCallback<T>>();
    }

    void setStrategy(BufferStrategy strategy) {
        this.strategy = strategy;
    }

    void addCallback(QueueBlockingCallback<T> callback) {
        callbacks.add(callback);
    }

    boolean save(T data) {
        int i = index.getAndIncrement();
        if (buffer[i] != null) {
            switch (strategy) {
                case BLOCKING:
                    boolean isFirstTimeBlocking = true;
                    while (buffer[i] != null) {// 自选等待当前位置被释放
                        if (isFirstTimeBlocking) { // 第一个阻塞在当前位置的线程，会通知所有注册的Callback
                            isFirstTimeBlocking = false;
                            for (QueueBlockingCallback<T> callback : callbacks) {
                                callback.notify(data);
                            }
                        }
                        try {
                            Thread.sleep(1L); // sleep 1 mills
                        } catch (InterruptedException e) {
                        }
                    }
                    break;
                case IF_POSSIBLE:
                    return false; // 直接返回false
                case OVERRIDE: // 啥都不做，直接走下面的赋值，覆盖旧数据
                default:
            }
        }
        buffer[i] = data; // 向当前位置填充数据
        return true;
    }

    public int getBufferSize() {
        return buffer.length;
    }

    public LinkedList<T> obtain() {
        return this.obtain(0, buffer.length);
    }

    public LinkedList<T> obtain(int start, int end) {
        LinkedList<T> result = new LinkedList<T>();
        // 将 start~end 之间的元素返回，消费者消费这个result集合就行了
        for (int i = start; i < end; i++) {
            if (buffer[i] != null) {
                result.add((T)buffer[i]);
                buffer[i] = null;
            }
        }
        return result;
    }

}
