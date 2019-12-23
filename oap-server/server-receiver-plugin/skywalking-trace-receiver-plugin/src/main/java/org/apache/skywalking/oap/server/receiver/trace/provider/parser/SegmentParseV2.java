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

package org.apache.skywalking.oap.server.receiver.trace.provider.parser;

import com.google.protobuf.InvalidProtocolBufferException;
import java.util.*;
import lombok.Setter;
import sun.management.Agent;

import org.apache.skywalking.apm.network.language.agent.*;
import org.apache.skywalking.apm.network.language.agent.v2.SegmentObject;
import org.apache.skywalking.oap.server.library.buffer.*;
import org.apache.skywalking.oap.server.library.module.ModuleManager;
import org.apache.skywalking.oap.server.core.analysis.TimeBucket;
import org.apache.skywalking.oap.server.receiver.trace.provider.TraceServiceModuleConfig;
import org.apache.skywalking.oap.server.receiver.trace.provider.parser.decorator.*;
import org.apache.skywalking.oap.server.receiver.trace.provider.parser.listener.*;
import org.apache.skywalking.oap.server.receiver.trace.provider.parser.standardization.*;
import org.apache.skywalking.oap.server.telemetry.TelemetryModule;
import org.apache.skywalking.oap.server.telemetry.api.*;
import org.slf4j.*;

/**
 * SegmentParseV2 is a replication of SegmentParse, but be compatible with v2 trace protocol.
 *
 * @author wusheng
 */
public class SegmentParseV2 {

    private static final Logger logger = LoggerFactory.getLogger(SegmentParseV2.class);

    private final ModuleManager moduleManager;
    // Span 监听器集合。通过不同的监听器，对 TraceSegment进行构建，生成不同的数据。在 #SegmentParse(ModuleManager) 构造方法 ，会看到它的初始化。
    private final List<SpanListener> spanListeners;
    private final SegmentParserListenerManager listenerManager;
    // 类似于一个数据传递的DTO
    private final SegmentCoreInfo segmentCoreInfo;
    private final TraceServiceModuleConfig config;
    @Setter private SegmentStandardizationWorker standardizationWorker;
    private volatile static CounterMetrics TRACE_BUFFER_FILE_RETRY;
    private volatile static CounterMetrics TRACE_BUFFER_FILE_OUT;
    private volatile static CounterMetrics TRACE_PARSE_ERROR;

    private SegmentParseV2(ModuleManager moduleManager, SegmentParserListenerManager listenerManager, TraceServiceModuleConfig config) {
        this.moduleManager = moduleManager;
        this.listenerManager = listenerManager;
        this.spanListeners = new LinkedList<>();
        this.segmentCoreInfo = new SegmentCoreInfo();
        this.segmentCoreInfo.setStartTime(Long.MAX_VALUE);
        this.segmentCoreInfo.setEndTime(Long.MIN_VALUE);
        this.segmentCoreInfo.setV2(true);
        this.config = config;
        // 解析Segment监控数据(略)
        if (TRACE_BUFFER_FILE_RETRY == null) {
            MetricsCreator metricsCreator = moduleManager.find(TelemetryModule.NAME).provider().getService(MetricsCreator.class);
            TRACE_BUFFER_FILE_RETRY = metricsCreator.createCounter("v6_trace_buffer_file_retry", "The number of retry trace segment from the buffer file, but haven't registered successfully.",
                MetricsTag.EMPTY_KEY, MetricsTag.EMPTY_VALUE);
            TRACE_BUFFER_FILE_OUT = metricsCreator.createCounter("v6_trace_buffer_file_out", "The number of trace segment out of the buffer file",
                MetricsTag.EMPTY_KEY, MetricsTag.EMPTY_VALUE);
            TRACE_PARSE_ERROR = metricsCreator.createCounter("v6_trace_parse_error", "The number of trace segment out of the buffer file",
                MetricsTag.EMPTY_KEY, MetricsTag.EMPTY_VALUE);
        }
    }

    public boolean parse(BufferData<UpstreamSegment> bufferData, SegmentSource source) {
        createSpanListeners();// 初始化Listener

        try {
            UpstreamSegment upstreamSegment = bufferData.getMessageType();
            // 获取该TraceSegment关联的全部TraceId
            List<UniqueId> traceIds = upstreamSegment.getGlobalTraceIdsList();
            // 反序列化UpstreamSegment.segment，得到SegmentObject对象
            if (bufferData.getV2Segment() == null) {
                bufferData.setV2Segment(parseBinarySegment(upstreamSegment));
            }
            SegmentObject segmentObject = bufferData.getV2Segment();
            // SegmentDecorator用来修改SegmentObject中数据的
            SegmentDecorator segmentDecorator = new SegmentDecorator(segmentObject);

            if (!preBuild(traceIds, segmentDecorator)) { // 预构建
                if (logger.isDebugEnabled()) {
                    logger.debug("This segment id exchange not success, write to buffer file, id: {}", segmentCoreInfo.getSegmentId());
                }

                if (source.equals(SegmentSource.Agent)) { // 预构建失败，写入Buffer文件中暂存
                    // 目前我们看到 TraceSegmentService 的调用使用的是 Source.Agent 。
                    // 而后台线程，定时调用该方法重新构建使用的是 Source.Buffer ，如果不加盖判断，会预构建失败重复写入。
                    writeToBufferFile(segmentCoreInfo.getSegmentId(), upstreamSegment);
                } else {
                    // from SegmentSource.Buffer
                    TRACE_BUFFER_FILE_RETRY.inc();
                }
                return false;
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("This segment id exchange success, id: {}", segmentCoreInfo.getSegmentId());
                }

                notifyListenerToBuild();
                return true;
            }
        } catch (Throwable e) {
            TRACE_PARSE_ERROR.inc();
            logger.error(e.getMessage(), e);
            return true;
        }
    }

    private SegmentObject parseBinarySegment(UpstreamSegment segment) throws InvalidProtocolBufferException {
        return SegmentObject.parseFrom(segment.getSegment());
    }

    private boolean preBuild(List<UniqueId> traceIds, SegmentDecorator segmentDecorator) {
        StringBuilder segmentIdBuilder = new StringBuilder();
        // 拼接segmentId
        for (int i = 0; i < segmentDecorator.getTraceSegmentId().getIdPartsList().size(); i++) {
            if (i == 0) {
                segmentIdBuilder.append(segmentDecorator.getTraceSegmentId().getIdPartsList().get(i));
            } else {
                segmentIdBuilder.append(".").append(segmentDecorator.getTraceSegmentId().getIdPartsList().get(i));
            }
        }
        // SpanListener处理UniqueId
        for (UniqueId uniqueId : traceIds) {
            notifyGlobalsListener(uniqueId);
        }
        // 填充SegmentCoreInfo，记录Segment的核心信息
        segmentCoreInfo.setSegmentId(segmentIdBuilder.toString());
        segmentCoreInfo.setServiceId(segmentDecorator.getServiceId());
        segmentCoreInfo.setServiceInstanceId(segmentDecorator.getServiceInstanceId());
        segmentCoreInfo.setDataBinary(segmentDecorator.toByteArray());
        segmentCoreInfo.setV2(true);

        boolean exchanged = true;

        for (int i = 0; i < segmentDecorator.getSpansCount(); i++) {
            SpanDecorator spanDecorator = segmentDecorator.getSpans(i); // 获取Span
            // IdExchanger就是将各个String转换成对应的id
            if (!SpanIdExchanger.getInstance(moduleManager).exchange(spanDecorator, segmentCoreInfo.getServiceId())) {
                exchanged = false;
            } else {
                for (int j = 0; j < spanDecorator.getRefsCount(); j++) {
                    ReferenceDecorator referenceDecorator = spanDecorator.getRefs(j);
                    if (!ReferenceIdExchanger.getInstance(moduleManager).exchange(referenceDecorator, segmentCoreInfo.getServiceId())) {
                        exchanged = false;
                    }
                }
            }
            // 调整Segment的startTime和endTime
            if (segmentCoreInfo.getStartTime() > spanDecorator.getStartTime()) {
                segmentCoreInfo.setStartTime(spanDecorator.getStartTime());
            }
            if (segmentCoreInfo.getEndTime() < spanDecorator.getEndTime()) {
                segmentCoreInfo.setEndTime(spanDecorator.getEndTime());
            }
            // 调整 Segment的isError字段
            segmentCoreInfo.setError(spanDecorator.getIsError() || segmentCoreInfo.isError());
        }

        if (exchanged) { // 已经完全替换成了相应的id
            // 更新minuteTimeBucket
            long minuteTimeBucket = TimeBucket.getMinuteTimeBucket(segmentCoreInfo.getStartTime());
            segmentCoreInfo.setMinuteTimeBucket(minuteTimeBucket);
            // 遍历全部Span，走notify*Listner()方法进行处理
            for (int i = 0; i < segmentDecorator.getSpansCount(); i++) {
                SpanDecorator spanDecorator = segmentDecorator.getSpans(i);

                if (spanDecorator.getSpanId() == 0) { // 解析第一个Span
                    notifyFirstListener(spanDecorator);
                }
                // 根据SpanType解析Span
                if (SpanType.Exit.equals(spanDecorator.getSpanType())) {
                    notifyExitListener(spanDecorator);
                } else if (SpanType.Entry.equals(spanDecorator.getSpanType())) {
                    notifyEntryListener(spanDecorator);
                } else if (SpanType.Local.equals(spanDecorator.getSpanType())) {
                    notifyLocalListener(spanDecorator);
                } else {
                    logger.error("span type value was unexpected, span type name: {}", spanDecorator.getSpanType().name());
                }
            }
        }

        return exchanged;
    }

    private void writeToBufferFile(String id, UpstreamSegment upstreamSegment) {
        if (logger.isDebugEnabled()) {
            logger.debug("push to segment buffer write worker, id: {}", id);
        }

        SegmentStandardization standardization = new SegmentStandardization(id);
        standardization.setUpstreamSegment(upstreamSegment);

        standardizationWorker.in(standardization);
    }

    private void notifyListenerToBuild() {
        spanListeners.forEach(SpanListener::build);
    }

    private void notifyExitListener(SpanDecorator spanDecorator) {
        spanListeners.forEach(listener -> {
            if (listener.containsPoint(SpanListener.Point.Exit)) {
                ((ExitSpanListener)listener).parseExit(spanDecorator, segmentCoreInfo);
            }
        });
    }

    private void notifyEntryListener(SpanDecorator spanDecorator) {
        spanListeners.forEach(listener -> {
            if (listener.containsPoint(SpanListener.Point.Entry)) {
                ((EntrySpanListener)listener).parseEntry(spanDecorator, segmentCoreInfo);
            }
        });
    }

    private void notifyLocalListener(SpanDecorator spanDecorator) {
        spanListeners.forEach(listener -> {
            if (listener.containsPoint(SpanListener.Point.Local)) {
                ((LocalSpanListener)listener).parseLocal(spanDecorator, segmentCoreInfo);
            }
        });
    }

    private void notifyFirstListener(SpanDecorator spanDecorator) {
        spanListeners.forEach(listener -> {
            if (listener.containsPoint(SpanListener.Point.First)) {
                ((FirstSpanListener)listener).parseFirst(spanDecorator, segmentCoreInfo);
            }
        });
    }

    private void notifyGlobalsListener(UniqueId uniqueId) {
        spanListeners.forEach(listener -> {
            if (listener.containsPoint(SpanListener.Point.TraceIds)) {
                // 使用 GlobalTraceSpanListener 处理链路追踪全局编号数组( TraceSegment.relatedGlobalTraces )。
                ((GlobalTraceIdsListener)listener).parseGlobalTraceId(uniqueId, segmentCoreInfo);
            }
        });
    }

    private void createSpanListeners() {
        listenerManager.getSpanListenerFactories().forEach(spanListenerFactory -> spanListeners.add(spanListenerFactory.create(moduleManager, config)));
    }

    public static class Producer implements DataStreamReader.CallBack<UpstreamSegment> {

        @Setter private SegmentStandardizationWorker standardizationWorker;
        private final ModuleManager moduleManager;
        private final SegmentParserListenerManager listenerManager;
        private final TraceServiceModuleConfig config;

        public Producer(ModuleManager moduleManager, SegmentParserListenerManager listenerManager, TraceServiceModuleConfig config) {
            this.moduleManager = moduleManager;
            this.listenerManager = listenerManager;
            this.config = config;
        }

        public void send(UpstreamSegment segment, SegmentSource source) {
            // 每个Segment都会新建一个SegmentParseV2对象进行解析
            SegmentParseV2 segmentParse = new SegmentParseV2(moduleManager, listenerManager, config);
            segmentParse.setStandardizationWorker(standardizationWorker);
            segmentParse.parse(new BufferData<>(segment), source);
        }

        @Override public boolean call(BufferData<UpstreamSegment> bufferData) {
            SegmentParseV2 segmentParse = new SegmentParseV2(moduleManager, listenerManager, config);
            segmentParse.setStandardizationWorker(standardizationWorker);
            boolean parseResult = segmentParse.parse(bufferData, SegmentSource.Buffer);
            if (parseResult) {
                TRACE_BUFFER_FILE_OUT.inc();
            }

            return parseResult;
        }
    }
}
