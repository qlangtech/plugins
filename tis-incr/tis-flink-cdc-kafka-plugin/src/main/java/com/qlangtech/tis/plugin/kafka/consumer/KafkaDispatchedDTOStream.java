/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.qlangtech.tis.plugin.kafka.consumer;

import com.qlangtech.tis.realtime.dto.DTOStream.DispatchedDTOStream;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.OutputTag;

import java.util.Map;
import java.util.Objects;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-01-04 10:11
 **/
public class KafkaDispatchedDTOStream extends DispatchedDTOStream {
    Map<String, DeserializationSchema<RowData>> tabColsMapper;
    private final String tabName;

    public KafkaDispatchedDTOStream(String tableName, Map<String, DeserializationSchema<RowData>> tabColsMapper) {
        super(RowData.class, new OutputTag<Tuple2<String, byte[]>>(tableName) {
        }, false);
        this.tabColsMapper = Objects.requireNonNull(tabColsMapper, "tabColsMapper");
        this.tabName = tableName;
    }

    @Override
    public DataStream getStream() {
        DeserializationSchema<RowData> deserializate = tabColsMapper.get(this.tabName);
        if (deserializate == null) {
            throw new IllegalStateException("table:" + this.tabName + " relevant deserializate can not be null,exist tabs in map"
                    + String.join(",", tabColsMapper.keySet()));
        }
        return super.getStream().map(new KafkaBytes2RowData(deserializate));
    }

    public static class KafkaBytes2RowData implements MapFunction<Tuple2<String, byte[]>, RowData> {
        private final DeserializationSchema<RowData> deserializate;

        public KafkaBytes2RowData(DeserializationSchema<RowData> deserializate) {
            this.deserializate = Objects.requireNonNull(deserializate, "deserializate can not be null");
        }

        @Override
        public RowData map(Tuple2<String, byte[]> value) throws Exception {
            return deserializate.deserialize(value.f1);
        }
    }
}
