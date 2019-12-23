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

package org.apache.skywalking.oap.server.receiver.trace.provider.parser.decorator;

import lombok.*;

/**
 * @author peng-yongsheng
 */
@Getter
@Setter
public class SegmentCoreInfo {
    // TraceSegment编号，即 TraceSegment.traceSegmentId 。
    private String segmentId;

    // Segment所属的Service以及ServiceInstance
    private int serviceId;
    private int serviceInstanceId;

    // Segment的开始时间和结束时间
    private long startTime;
    private long endTime;

    // 如果TraceSegment范围内的任意一个Span被标记了Error，则该字段会被设置为true
    private boolean isError;

    // TraceSegment开始时间窗口(即第一个Span开始时间所处的分钟级时间窗口)
    private long minuteTimeBucket;

    // 整个TraceSegment的数据
    private byte[] dataBinary;
    // 是否为V2版本的TraceSegment
    private boolean isV2;
}
