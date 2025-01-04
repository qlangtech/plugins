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

import com.qlangtech.tis.plugin.datax.format.guesstype.StructuredReader.StructuredRecord;
import com.qlangtech.tis.plugins.incr.flink.chunjun.kafka.format.FormatFactory;
import com.qlangtech.tis.realtime.transfer.DTO;
import com.qlangtech.tis.realtime.transfer.DTO.EventType;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.Objects;

/**
 * kafka 中获得的消息，需要先经过一次分流，然后再使用 flink 提供的 deserialize解析成rowdata
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-12-27 13:56
 * @see org.apache.flink.formats.json.canal.CanalJsonDeserializationSchema
 **/
public class KafkaDTODeserializationSchema implements DeserializationSchema<DTO> {
    private final FormatFactory msgFormat;

    private final KafkaStructuredRecord reuseReocrd;

    public KafkaDTODeserializationSchema(FormatFactory msgFormat) {
        this.msgFormat = Objects.requireNonNull(msgFormat, "msgFormat can not be null");
        this.reuseReocrd = new KafkaStructuredRecord();
    }

    @Override
    public DTO deserialize(byte[] message) throws IOException {
        throw new UnsupportedEncodingException("please use deserialize(byte[] message, Collector<DTO> out) instead");
    }

    @Override
    public void deserialize(byte[] message, Collector<DTO> out) throws IOException {
        DTO dto = null;
        KafkaStructuredRecord record = (KafkaStructuredRecord) msgFormat.parseRecord(reuseReocrd, message);
        if (record == null) {
            return;
        }
        dto = new DTO();
        dto.setTableName(record.tabName);
        EventType event = record.getEventType();
        dto.setEventType(event);

        if (event == null) {
            throw new IllegalStateException("event can not be null");
        }
        Map<String, Object> afterVals = record.vals;
        Map<String, Object> beforeVals = record.getOldVals();
        switch (event) {
            case DELETE: {
                dto.setBefore(beforeVals);
                out.collect(dto);
                break;
            }
            case ADD: {
                dto.setAfter(afterVals);
                out.collect(dto);
                break;
            }
            case UPDATE_AFTER: {

                dto.setAfter(afterVals);
                dto.setBefore(beforeVals);
                dto.setEventType(EventType.UPDATE_AFTER);
                out.collect(dto);

                out.collect(dto.clone().setEventType(EventType.UPDATE_BEFORE));
                break;
            }
            default:
                throw new IllegalStateException("unsupport event:" + event);
        }


//        dto.setBefore(beforeVals);
//        dto.setAfter(afterVals);

        // return Tuple2.of(record.tabName, message);
      //  return dto;
    }

    @Override
    public boolean isEndOfStream(DTO nextElement) {
        return false;
    }

    @Override
    public TypeInformation<DTO> getProducedType() {
        // return new TupleTypeInfo<>(Types.STRING, Types.PRIMITIVE_ARRAY(TypeInformation.of(Byte.TYPE)));
        return TypeInformation.of(DTO.class);
    }
}
