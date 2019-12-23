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

package org.apache.skywalking.oap.server.core.analysis.metrics;

import java.util.*;
import lombok.*;
import org.apache.skywalking.oap.server.core.analysis.metrics.annotation.*;
import org.apache.skywalking.oap.server.core.query.sql.Function;
import org.apache.skywalking.oap.server.core.storage.annotation.Column;

/**
 * PxxMetrics is a parent metrics for p99/p95/p90/p75/p50 metrics. P(xx) metrics is also for P(xx) percentile.
 *
 * A percentile (or a centile) is a measure used in statistics indicating the value below which a given percentage of
 * observations in a group of observations fall. For example, the 20th percentile is the value (or score) below which
 * 20% of the observations may be found.
 *
 * @author wusheng, peng-yongsheng
 */
public abstract class PxxMetrics extends Metrics implements IntValueHolder {
    protected static final String DETAIL_GROUP = "detail_group";
    protected static final String VALUE = "value";
    protected static final String PRECISION = "precision";

    @Getter @Setter @Column(columnName = VALUE, isValue = true, function = Function.Avg) private int value;
    @Getter @Setter @Column(columnName = PRECISION) private int precision;
    @Getter @Setter @Column(columnName = DETAIL_GROUP) private IntKeyLongValueArray detailGroup;

    private final int percentileRank;
    private Map<Integer, IntKeyLongValue> detailIndex;

    public PxxMetrics(int percentileRank) {
        this.percentileRank = percentileRank;
        detailGroup = new IntKeyLongValueArray(30);
    }

    @Entrance
    public final void combine(@SourceFrom int value, @Arg int precision) {
        this.precision = precision; // 确定监控精度，默认为10，即10毫秒级别

        this.indexCheckAndInit();
        // 初始化detailIndex这个Map

        int index = value / precision;
        IntKeyLongValue element = detailIndex.get(index);
        if (element == null) { // 创建IntKeyLongValue对象
            element = new IntKeyLongValue();
            element.setKey(index);
            element.setValue(1);
            addElement(element); // 记录到detailGroup和detailIndex集合中
        } else {
            element.addValue(1); // 递增value
        }
    }

    @Override
    public void combine(Metrics metrics) {
        PxxMetrics pxxMetrics = (PxxMetrics)metrics;
        this.indexCheckAndInit();
        pxxMetrics.indexCheckAndInit();

        pxxMetrics.detailIndex.forEach((key, element) -> {
            IntKeyLongValue existingElement = this.detailIndex.get(key);
            if (existingElement == null) {
                existingElement = new IntKeyLongValue();
                existingElement.setKey(key);
                existingElement.setValue(element.getValue());
                addElement(element);
            } else {
                existingElement.addValue(element.getValue());
            }
        });
    }

    @Override
    public final void calculate() {
        Collections.sort(detailGroup); // 排序detailGroup
        // 计算该窗口监控点的总个数
        int total = detailGroup.stream().mapToInt(element -> (int)element.getValue()).sum();
        // 查找指定分位数的位置
        int roof = Math.round(total * percentileRank * 1.0f / 100);

        int count = 0;
        for (IntKeyLongValue element : detailGroup) {
            // 累加监控点个数，直至到达(或超过)上面的roof值，此时的监控值即为指定分位数监控值
            count += element.getValue();
            if (count >= roof) {
                value = element.getKey() * precision;
                return;
            }
        }
    }

    private void addElement(IntKeyLongValue element) {
        detailGroup.add(element);
        detailIndex.put(element.getKey(), element);
    }

    private void indexCheckAndInit() {
        if (detailIndex == null) { // 初始化
            detailIndex = new HashMap<>();
            detailGroup.forEach(element -> detailIndex.put(element.getKey(), element));
        }
    }
}
