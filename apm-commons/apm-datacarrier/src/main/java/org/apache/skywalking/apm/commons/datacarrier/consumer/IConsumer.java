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


package org.apache.skywalking.apm.commons.datacarrier.consumer;

import java.util.List;

/**
 * Created by wusheng on 2016/10/25.
 */
public interface IConsumer<T> {
    // 初始化消费者。
    void init();

    // 批量消费消息。
    void consume(List<T> data);

    // 处理当消费发生异常。
    void onError(List<T> data, Throwable t);

    // 处理当消费结束，关闭ConsumerThread
    void onExit();
}
