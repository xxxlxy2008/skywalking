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
import lombok.Getter;
import org.apache.skywalking.oap.server.core.*;
import org.apache.skywalking.oap.server.core.analysis.*;
import org.apache.skywalking.oap.server.core.analysis.metrics.Metrics;
import org.apache.skywalking.oap.server.core.config.DownsamplingConfigService;
import org.apache.skywalking.oap.server.core.remote.define.StreamDataMappingSetter;
import org.apache.skywalking.oap.server.core.storage.*;
import org.apache.skywalking.oap.server.core.storage.annotation.Storage;
import org.apache.skywalking.oap.server.core.storage.model.*;
import org.apache.skywalking.oap.server.library.module.ModuleDefineHolder;

/**
 * @author peng-yongsheng
 */
public class MetricsStreamProcessor implements StreamProcessor<Metrics> {

    private final static MetricsStreamProcessor PROCESSOR = new MetricsStreamProcessor();

    private Map<Class<? extends Metrics>, MetricsAggregateWorker> entryWorkers = new HashMap<>();
    @Getter private List<MetricsPersistentWorker> persistentWorkers = new ArrayList<>();

    public static MetricsStreamProcessor getInstance() {
        return PROCESSOR;
    }

    public void in(Metrics metrics) {
        MetricsAggregateWorker worker = entryWorkers.get(metrics.getClass());
        if (worker != null) {
            worker.in(metrics);
        }
    }

    public void create(ModuleDefineHolder moduleDefineHolder, Stream stream, Class<? extends Metrics> metricsClass) {
        if (DisableRegister.INSTANCE.include(stream.name())) {
            return;
        }
        // 获取 IMetricsDAO
        StorageDAO storageDAO = moduleDefineHolder.find(StorageModule.NAME).provider().getService(StorageDAO.class);
        IMetricsDAO metricsDAO;
        try {
            metricsDAO = storageDAO.newMetricsDao(stream.builder().newInstance());
        } catch (InstantiationException | IllegalAccessException e) {
            throw new UnexpectedException("Create " + stream.builder().getSimpleName() + " metrics DAO failure.", e);
        }

        IModelSetter modelSetter = moduleDefineHolder.find(CoreModule.NAME).provider().getService(IModelSetter.class);
        DownsamplingConfigService configService = moduleDefineHolder.find(CoreModule.NAME).provider().getService(DownsamplingConfigService.class);

        StreamDataMappingSetter streamDataMappingSetter = moduleDefineHolder.find(CoreModule.NAME).provider().getService(StreamDataMappingSetter.class);
        streamDataMappingSetter.putIfAbsent(metricsClass);

        MetricsPersistentWorker hourPersistentWorker = null;
        MetricsPersistentWorker dayPersistentWorker = null;
        MetricsPersistentWorker monthPersistentWorker = null;

        if (configService.shouldToHour()) {
            Model model = modelSetter.putIfAbsent(metricsClass, stream.scopeId(), new Storage(stream.name(), true, true, Downsampling.Hour));
            hourPersistentWorker = worker(moduleDefineHolder, metricsDAO, model);
        }
        if (configService.shouldToDay()) {
            Model model = modelSetter.putIfAbsent(metricsClass, stream.scopeId(), new Storage(stream.name(), true, true, Downsampling.Day));
            dayPersistentWorker = worker(moduleDefineHolder, metricsDAO, model);
        }
        if (configService.shouldToMonth()) {
            Model model = modelSetter.putIfAbsent(metricsClass, stream.scopeId(), new Storage(stream.name(), true, true, Downsampling.Month));
            monthPersistentWorker = worker(moduleDefineHolder, metricsDAO, model);
        }

        Model model = modelSetter.putIfAbsent(metricsClass, stream.scopeId(), new Storage(stream.name(), true, true, Downsampling.Minute));
        // 创建minutePersistentWorker
        MetricsPersistentWorker minutePersistentWorker = minutePersistentWorker(moduleDefineHolder, metricsDAO, model);
        // 创建 MetricsTransWorker，后续worker指向 minutePersistenceWorker对象(以及hour、day、monthPersistentWorker)
        MetricsTransWorker transWorker = new MetricsTransWorker(moduleDefineHolder, stream.name(),
                minutePersistentWorker, hourPersistentWorker, dayPersistentWorker, monthPersistentWorker);
        // 创建MetricsRemoteWorker，并将 nextWorker指向上面的 MetricsTransWorker对象
        MetricsRemoteWorker remoteWorker = new MetricsRemoteWorker(moduleDefineHolder, transWorker, stream.name());
        // 创建MetricsAggregateWorker，并将nextWorker指向上面的MetricsRemoteWorker对象
        MetricsAggregateWorker aggregateWorker = new MetricsAggregateWorker(moduleDefineHolder, remoteWorker, stream.name());
        // 将上述worker链与指定 Metrics类型绑定
        entryWorkers.put(metricsClass, aggregateWorker);
    }

    private MetricsPersistentWorker minutePersistentWorker(ModuleDefineHolder moduleDefineHolder, IMetricsDAO metricsDAO, Model model) {
        AlarmNotifyWorker alarmNotifyWorker = new AlarmNotifyWorker(moduleDefineHolder);
        ExportWorker exportWorker = new ExportWorker(moduleDefineHolder);

        MetricsPersistentWorker minutePersistentWorker = new MetricsPersistentWorker(moduleDefineHolder, model,
            1000, metricsDAO, alarmNotifyWorker, exportWorker);
        persistentWorkers.add(minutePersistentWorker);

        return minutePersistentWorker;
    }

    private MetricsPersistentWorker worker(ModuleDefineHolder moduleDefineHolder, IMetricsDAO metricsDAO, Model model) {
        MetricsPersistentWorker persistentWorker = new MetricsPersistentWorker(moduleDefineHolder, model,
            1000, metricsDAO, null, null);
        persistentWorkers.add(persistentWorker);

        return persistentWorker;
    }
}
