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
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;

import java.io.IOException;
import java.util.Objects;

/**
 * kafka 中获得的消息，需要先经过一次分流，然后再使用 flink 提供的 deserialize解析成rowdata
 *
 * @see org.apache.flink.formats.json.canal.CanalJsonDeserializationSchema
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-12-27 13:56
 **/
public class KafkaDeserializationSchema implements DeserializationSchema<Tuple2<String, byte[]>> {
    private final FormatFactory msgFormat;

    private final KafkaStructuredRecord reuseReocrd;

    public KafkaDeserializationSchema(FormatFactory msgFormat) {
        this.msgFormat = Objects.requireNonNull(msgFormat, "msgFormat can not be null");
        this.reuseReocrd = new KafkaStructuredRecord();
    }

    @Override
    public Tuple2<String, byte[]> deserialize(byte[] message) throws IOException {
        StructuredRecord record = msgFormat.parseRecord(reuseReocrd, message);
        return Tuple2.of(record.tabName, message);
    }

    @Override
    public boolean isEndOfStream(Tuple2<String, byte[]> nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Tuple2<String, byte[]>> getProducedType() {
        return new TupleTypeInfo<>(Types.STRING, Types.PRIMITIVE_ARRAY(TypeInformation.of(Byte.TYPE)));
    }
}
