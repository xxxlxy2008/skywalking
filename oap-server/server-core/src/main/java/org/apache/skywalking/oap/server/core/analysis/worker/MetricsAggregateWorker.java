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
import org.apache.skywalking.apm.commons.datacarrier.*;
import org.apache.skywalking.apm.commons.datacarrier.consumer.*;
import org.apache.skywalking.oap.server.core.UnexpectedException;
import org.apache.skywalking.oap.server.core.analysis.data.*;
import org.apache.skywalking.oap.server.core.analysis.metrics.Metrics;
import org.apache.skywalking.oap.server.core.worker.AbstractWorker;
import org.apache.skywalking.oap.server.library.module.ModuleDefineHolder;
import org.apache.skywalking.oap.server.telemetry.TelemetryModule;
import org.apache.skywalking.oap.server.telemetry.api.*;
import org.slf4j.*;

/**
 * @author peng-yongsheng
 */
public class MetricsAggregateWorker extends AbstractWorker<Metrics> {

    private static final Logger logger = LoggerFactory.getLogger(MetricsAggregateWorker.class);

    private AbstractWorker<Metrics> nextWorker;
    private final DataCarrier<Metrics> dataCarrier;
    private final MergeDataCache<Metrics> mergeDataCache;
    private final String modelName;
    private CounterMetrics aggregationCounter;
    private final long l2AggregationSendCycle;
    private long lastSendTimestamp;

    MetricsAggregateWorker(ModuleDefineHolder moduleDefineHolder, AbstractWorker<Metrics> nextWorker,
        String modelName) {
        super(moduleDefineHolder);
        this.modelName = modelName;
        this.nextWorker = nextWorker;
        this.mergeDataCache = new MergeDataCache<>();
        String name = "METRICS_L1_AGGREGATION";
        this.dataCarrier = new DataCarrier<>("MetricsAggregateWorker." + modelName, name, 2, 10000);

        BulkConsumePool.Creator creator = new BulkConsumePool.Creator(name, BulkConsumePool.Creator.recommendMaxSize() * 2, 20);
        try {
            ConsumerPoolFactory.INSTANCE.createIfAbsent(name, creator);
        } catch (Exception e) {
            throw new UnexpectedException(e.getMessage(), e);
        }
        this.dataCarrier.consume(ConsumerPoolFactory.INSTANCE.get(name), new AggregatorConsumer(this));

        MetricsCreator metricsCreator = moduleDefineHolder.find(TelemetryModule.NAME).provider().getService(MetricsCreator.class);
        aggregationCounter = metricsCreator.createCounter("metrics_aggregation", "The number of rows in aggregation",
            new MetricsTag.Keys("metricName", "level", "dimensionality"), new MetricsTag.Values(modelName, "1", "min"));
        lastSendTimestamp = System.currentTimeMillis();

        l2AggregationSendCycle = EnvUtil.getLong("METRICS_L1_AGGREGATION_SEND_CYCLE", 1000);
    }

    @Override public final void in(Metrics metrics) {
        metrics.setEndOfBatchContext(new EndOfBatchContext(false));
        dataCarrier.produce(metrics);
    }

    private void onWork(Metrics metrics) {
        aggregationCounter.inc(); // 记录监控，底层按照 Promethus的格式记录，后面详细分析
        aggregate(metrics); // 进行

        if (metrics.getEndOfBatchContext().isEndOfBatch()) {
            if (shouldSend()) {
                sendToNext();
            }
        }
    }

    private boolean shouldSend() {
        long now = System.currentTimeMillis();
        // Continue L2 aggregation in certain cycle.
        if (now - lastSendTimestamp > l2AggregationSendCycle) {
            lastSendTimestamp = now;
            return true;
        }
        return false;
    }

    private void sendToNext() {
        // 首先进行队列切换，之后会设置 last队列的 reading状态
        mergeDataCache.switchPointer();
        // 此时可能其他的 Consumer线程还在写入 last队列，需要等待写入完成
        while (mergeDataCache.getLast().isWriting()) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
            }
        }
        // 开始读取 last队列中的全部 Metrics数据并发送到下一个 worker处理
        mergeDataCache.getLast().collection().forEach(data -> {
            if (logger.isDebugEnabled()) {
                logger.debug(data.toString());
            }

            nextWorker.in(data);
        });
        // 读取完成后，清空 last队列以及其 reading状态
        mergeDataCache.finishReadingLast();
    }

    private void aggregate(Metrics metrics) {
        mergeDataCache.writing(); // 将 lockedMergeDataCollection指向 current队列，并设置其 writing标记
        if (mergeDataCache.containsKey(metrics)) { // 存在重复的监控数据数据，则进行合并
            mergeDataCache.get(metrics).combine(metrics);
        } else { //
            mergeDataCache.put(metrics);
        }
        // 清理 current队列的 writing标记，之后清理 lockedMergeDataCollection
        mergeDataCache.finishWriting();
    }

    private class AggregatorConsumer implements IConsumer<Metrics> {

        private final MetricsAggregateWorker aggregator;

        private AggregatorConsumer(MetricsAggregateWorker aggregator) {
            this.aggregator = aggregator;
        }

        @Override public void init() {

        }

        @Override public void consume(List<Metrics> data) {
            Iterator<Metrics> inputIterator = data.iterator();

            int i = 0;
            while (inputIterator.hasNext()) {
                Metrics metrics = inputIterator.next();
                i++;
                if (i == data.size()) {
                    metrics.getEndOfBatchContext().setEndOfBatch(true);
                }
                aggregator.onWork(metrics);
            }
        }

        @Override public void onError(List<Metrics> data, Throwable t) {
            logger.error(t.getMessage(), t);
        }

        @Override public void onExit() {
        }
    }
}
