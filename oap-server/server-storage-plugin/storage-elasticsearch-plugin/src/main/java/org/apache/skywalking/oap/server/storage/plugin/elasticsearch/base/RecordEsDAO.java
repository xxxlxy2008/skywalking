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

package org.apache.skywalking.oap.server.storage.plugin.elasticsearch.base;

import java.io.IOException;
import org.apache.skywalking.oap.server.core.analysis.record.Record;
import org.apache.skywalking.oap.server.core.storage.*;
import org.apache.skywalking.oap.server.core.storage.model.Model;
import org.apache.skywalking.oap.server.library.client.elasticsearch.ElasticSearchClient;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;

/**
 * @author peng-yongsheng
 */
public class RecordEsDAO extends EsDAO implements IRecordDAO<IndexRequest> {

    private final StorageBuilder<Record> storageBuilder;

    RecordEsDAO(ElasticSearchClient client, StorageBuilder<Record> storageBuilder) {
        super(client);
        this.storageBuilder = storageBuilder;
    }

    @Override public IndexRequest prepareBatchInsert(Model model, Record record) throws IOException {
        XContentBuilder builder = map2builder(storageBuilder.data2Map(record));
        // 生成的是最终Index名称，这里的Index由前缀字符串(即"segment")+TimeBucket两部分构成
        String modelName = TimeSeriesUtils.timeSeries(model, record.getTimeBucket());
        // 创建IndexRequest
        return getClient().prepareInsert(modelName, record.id(), builder);
    }
}
