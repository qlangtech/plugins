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

package com.qlangtech.tis.plugins.incr.flink.chunjun.kafka.sink;

import com.dtstack.chunjun.connector.kafka.sink.KafkaProducer;
import com.qlangtech.tis.plugins.datax.kafka.writer.KafkaProducerFactory;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.streaming.connectors.kafka.internals.FlinkKafkaInternalProducer;
import org.apache.flink.table.data.RowData;

import java.util.Properties;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-03-21 11:05
 **/
public class TISKafkaProducer extends KafkaProducer {
    public TISKafkaProducer(String defaultTopic, KafkaSerializationSchema<RowData> serializationSchema
            , Properties producerConfig, Semantic semantic, int kafkaProducersPoolSize) {
        super(defaultTopic, serializationSchema, producerConfig, semantic, kafkaProducersPoolSize);
    }

    @Override
    protected FlinkKafkaInternalProducer<byte[], byte[]> createProducer() {
        return KafkaProducerFactory.withCurrentThreadSetter(() -> {
            return new FlinkKafkaInternalProducer(this.producerConfig);
        });
    }

}
