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

package com.qlangtech.tis.plugins.incr.flink.chunjun.kafka.format.debeziumjson;

import com.alibaba.fastjson.JSON;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.kafka.consumer.KafkaStructuredRecord;
import com.qlangtech.tis.realtime.transfer.DTO.EventType;

import java.util.HashMap;
import java.util.Map;

/**
 * https://debezium.io/documentation/reference/3.0/tutorial.html#viewing-change-events
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-01-02 12:06
 **/
public class TISSourceDebeziumJsonFormatFactory extends TISSinkDebeziumJsonFormatFactory {

    private static final String FIELD_AFTER = "after";
    private static final String FIELD_BEFORE = "before";
    private static final String OP_INSERT = "c";
    private static final String OP_UPDATE = "u";
    private static final String OP_DELETE = "d";
    private static final String OP_READ = "r";

    @Override
    public KafkaStructuredRecord parseRecord(KafkaStructuredRecord reuse, byte[] record) {
        reuse.clean();
        HashMap jsonObject = JSON.parseObject(record, HashMap.class);
        Object operation = jsonObject.get("op");
        Map source = (Map) jsonObject.get("source");
        if (operation == null || source == null) {
            return null;
        }

        switch ((String) operation) {
            case OP_INSERT:
            case OP_READ:
                reuse.setEventType(EventType.ADD);
                break;
            case OP_UPDATE:
                reuse.setEventType(EventType.UPDATE_AFTER);
                break;
            case OP_DELETE:
                reuse.setEventType(EventType.DELETE);
                break;
            default:
                throw new IllegalStateException("illegal operation:" + operation);
        }

        reuse.setVals((Map<String, Object>) jsonObject.get(FIELD_AFTER));
        reuse.setOldVals((Map<String, Object>) jsonObject.get(FIELD_BEFORE));
        /**
         *  <pre>
         * 	"source": {
         * 		"db": "order2",
         * 		"table": "orderdetail_01"
         *    }
         *  <pre/>
         */
        reuse.setTabName((String) source.get("table"));
        return reuse;
    }

//    @Override
//    public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(String targetTabName) {
//        return this.createFormat(targetTabName, (factory, cfg) -> factory.createDecodingFormat(null, cfg));
//    }

    @TISExtension
    public static final class DftDescriptor extends TISSinkDebeziumJsonFormatFactory.DftDescriptor {
        public DftDescriptor() {
            super();
        }




        @Override
        public EndType getEndType() {
            return EndType.SOURCE;
        }
    }
}
