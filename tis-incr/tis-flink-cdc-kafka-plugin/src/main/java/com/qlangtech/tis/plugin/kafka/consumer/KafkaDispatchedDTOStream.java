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
import com.qlangtech.tis.realtime.transfer.DTO;
import org.apache.flink.util.OutputTag;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-01-04 10:11
 **/
public class KafkaDispatchedDTOStream extends DispatchedDTOStream<DTO> {
//    Map<String, DeserializationSchema<RowData>> tabColsMapper;
//    private final String tabName;

    public KafkaDispatchedDTOStream(String tableName, boolean startNewChain) {
        super(DTO.class, new OutputTag<DTO>(tableName) {
        }, startNewChain);
        // this.tabColsMapper = Objects.requireNonNull(tabColsMapper, "tabColsMapper");
        // this.tabName = tableName;
    }

//    @Override
//    public DataStream getStream() {
//        DeserializationSchema<RowData> deserializate = tabColsMapper.get(this.tabName);
//        if (deserializate == null) {
//            throw new IllegalStateException("table:" + this.tabName + " relevant deserializate can not be null,exist tabs in map"
//                    + String.join(",", tabColsMapper.keySet()));
//        }
//        return super.getStream().map(new KafkaBytes2RowData(deserializate));
//    }

//    public static class KafkaBytes2RowData implements MapFunction<Tuple2<String, byte[]>, RowData> {
//        private final DeserializationSchema<RowData> deserializate;
//
//        public KafkaBytes2RowData(DeserializationSchema<RowData> deserializate) {
//            this.deserializate = Objects.requireNonNull(deserializate, "deserializate can not be null");
//        }
//
//        @Override
//        public RowData map(Tuple2<String, byte[]> value) throws Exception {
//            return deserializate.deserialize(value.f1);
//        }
//    }
}
