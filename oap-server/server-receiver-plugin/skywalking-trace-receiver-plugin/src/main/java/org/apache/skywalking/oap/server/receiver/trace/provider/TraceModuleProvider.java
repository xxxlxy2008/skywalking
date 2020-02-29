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

package org.apache.skywalking.oap.server.receiver.trace.provider;

import java.io.IOException;

import org.apache.skywalking.apm.util.StringUtil;
import org.apache.skywalking.oap.server.configuration.api.*;
import org.apache.skywalking.oap.server.core.CoreModule;
import org.apache.skywalking.oap.server.core.server.*;
import org.apache.skywalking.oap.server.library.module.*;
import org.apache.skywalking.oap.server.receiver.sharing.server.SharingServerModule;
import org.apache.skywalking.oap.server.receiver.trace.module.TraceModule;
import org.apache.skywalking.oap.server.receiver.trace.provider.handler.kafka.TraceSegmentReportServiceConsumer;
import org.apache.skywalking.oap.server.receiver.trace.provider.handler.v5.grpc.TraceSegmentServiceHandler;
import org.apache.skywalking.oap.server.receiver.trace.provider.handler.v5.rest.TraceSegmentServletHandler;
import org.apache.skywalking.oap.server.receiver.trace.provider.handler.v6.grpc.TraceSegmentReportServiceHandler;
import org.apache.skywalking.oap.server.receiver.trace.provider.parser.*;
import org.apache.skywalking.oap.server.receiver.trace.provider.parser.listener.endpoint.MultiScopesSpanListener;
import org.apache.skywalking.oap.server.receiver.trace.provider.parser.listener.segment.SegmentSpanListener;
import org.apache.skywalking.oap.server.receiver.trace.provider.parser.listener.service.ServiceMappingSpanListener;
import org.apache.skywalking.oap.server.receiver.trace.provider.parser.standardization.SegmentStandardizationWorker;
import org.apache.skywalking.oap.server.telemetry.TelemetryModule;

/**
 * @author peng-yongsheng
 */
public class TraceModuleProvider extends ModuleProvider {

    private final TraceServiceModuleConfig moduleConfig;
    private SegmentParse.Producer segmentProducer;
    private SegmentParseV2.Producer segmentProducerV2;
    private DBLatencyThresholdsAndWatcher thresholds;
    private TraceSegmentReportServiceConsumer segmentReportServiceConsumer;

    public TraceModuleProvider() {
        this.moduleConfig = new TraceServiceModuleConfig();
    }

    @Override public String name() {
        return "default";
    }

    @Override public Class<? extends ModuleDefine> module() {
        return TraceModule.class;
    }

    @Override public ModuleConfig createConfigBeanIfAbsent() {
        return moduleConfig;
    }

    @Override public void prepare() throws ServiceNotProvidedException {
        // 用来监听慢查询的
        thresholds = new DBLatencyThresholdsAndWatcher(moduleConfig.getSlowDBAccessThreshold(), this);

        moduleConfig.setDbLatencyThresholdsAndWatcher(thresholds);

        SegmentParserListenerManager listenerManager = new SegmentParserListenerManager();
        if (moduleConfig.isTraceAnalysis()) {
            listenerManager.add(new MultiScopesSpanListener.Factory());
            listenerManager.add(new ServiceMappingSpanListener.Factory());
        }
        listenerManager.add(new SegmentSpanListener.Factory(moduleConfig.getSampleRate()));

        segmentProducer = new SegmentParse.Producer(getManager(), listenerManager, moduleConfig);

        listenerManager = new SegmentParserListenerManager();
        if (moduleConfig.isTraceAnalysis()) {
            listenerManager.add(new MultiScopesSpanListener.Factory());
            listenerManager.add(new ServiceMappingSpanListener.Factory());
        }
        listenerManager.add(new SegmentSpanListener.Factory(moduleConfig.getSampleRate()));

        segmentProducerV2 = new SegmentParseV2.Producer(getManager(), listenerManager, moduleConfig);

        this.registerServiceImplementation(ISegmentParserService.class, new SegmentParserServiceImpl(segmentProducerV2));
    }

    @Override public void start() throws ModuleStartException {
        DynamicConfigurationService dynamicConfigurationService = getManager().find(ConfigurationModule.NAME).provider().getService(DynamicConfigurationService.class);
        GRPCHandlerRegister grpcHandlerRegister = getManager().find(SharingServerModule.NAME).provider().getService(GRPCHandlerRegister.class);
        JettyHandlerRegister jettyHandlerRegister = getManager().find(SharingServerModule.NAME).provider().getService(JettyHandlerRegister.class);
        try {
            dynamicConfigurationService.registerConfigChangeWatcher(thresholds);

            grpcHandlerRegister.addHandler(new TraceSegmentServiceHandler(segmentProducer));
            grpcHandlerRegister.addHandler(new TraceSegmentReportServiceHandler(segmentProducerV2, getManager()));
            String reportStrategy = moduleConfig.getReportStrategy();
            if(!StringUtil.isEmpty(reportStrategy) && "kafka".equals(reportStrategy.toLowerCase())){
                segmentReportServiceConsumer = new TraceSegmentReportServiceConsumer(segmentProducerV2,moduleConfig.getKafkaTopic());
                segmentReportServiceConsumer.start();
            }
            jettyHandlerRegister.addHandler(new TraceSegmentServletHandler(segmentProducer));

            SegmentStandardizationWorker standardizationWorker = new SegmentStandardizationWorker(getManager(), segmentProducer,
                moduleConfig.getBufferPath() + "v5", moduleConfig.getBufferOffsetMaxFileSize(), moduleConfig.getBufferDataMaxFileSize(), moduleConfig.isBufferFileCleanWhenRestart(),
                false);
            segmentProducer.setStandardizationWorker(standardizationWorker);

            SegmentStandardizationWorker standardizationWorker2 = new SegmentStandardizationWorker(getManager(), segmentProducer,
                moduleConfig.getBufferPath(), moduleConfig.getBufferOffsetMaxFileSize(), moduleConfig.getBufferDataMaxFileSize(), moduleConfig.isBufferFileCleanWhenRestart(),
                true);
            segmentProducerV2.setStandardizationWorker(standardizationWorker2);
        } catch (IOException e) {
            throw new ModuleStartException(e.getMessage(), e);
        }
    }

    @Override public void notifyAfterCompleted() {

    }

    @Override public String[] requiredModules() {
        return new String[] {TelemetryModule.NAME, CoreModule.NAME, SharingServerModule.NAME, ConfigurationModule.NAME};
    }
}
