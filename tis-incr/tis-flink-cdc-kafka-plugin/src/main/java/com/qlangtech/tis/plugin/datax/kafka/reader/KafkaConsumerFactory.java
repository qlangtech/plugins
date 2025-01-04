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

package com.qlangtech.tis.plugin.datax.kafka.reader;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

public class KafkaConsumerFactory {


    private final KafkaConsumer<byte[], byte[]> consumer;


    private KafkaConsumerFactory(final DataXKafkaReader config
            , Function<Map<String, Object>, Map<String, Object>> configRewrite //
            , boolean isTest) {
        this.consumer = withCurrentThreadSetter(() -> {
            return new KafkaConsumer<byte[], byte[]>(configRewrite.apply(config.buildKafkaConfig(isTest)));
        });
    }


    public static <T> T withCurrentThreadSetter(Supplier<T> instanceCreator) {
        final ClassLoader currentLoader = Thread.currentThread().getContextClassLoader();
        try {
            // 创建KafkaProducer过程中，创建 org.apache.kafka.common.serialization.ByteArraySerializer需要使用当前的classloader，不然会加载不到
            Thread.currentThread().setContextClassLoader(KafkaConsumerFactory.class.getClassLoader());
            return instanceCreator.get();
        } finally {
            Thread.currentThread().setContextClassLoader(currentLoader);
        }
    }

    public static KafkaConsumerFactory getKafkaConfig(final DataXKafkaReader config, boolean isTest) {
        return getKafkaConfig(config, (props) -> props, isTest);
    }

    public static KafkaConsumerFactory getKafkaConfig(
            final DataXKafkaReader config, Function<Map<String, Object>, Map<String, Object>> configRewrite, boolean isTest) {
        return new KafkaConsumerFactory(config, configRewrite, isTest);
    }

    //
//
//    public String getTopicPattern() {
//        return topicPattern;
//    }
//
//    public boolean isSync() {
//        return sync;
//    }
//
    public KafkaConsumer<byte[], byte[]> getConsumer() {
        return this.consumer;
    }

}
