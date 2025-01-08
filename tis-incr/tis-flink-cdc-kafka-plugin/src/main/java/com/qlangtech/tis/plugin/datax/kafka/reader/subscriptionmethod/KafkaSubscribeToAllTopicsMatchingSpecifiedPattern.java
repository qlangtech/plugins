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

package com.qlangtech.tis.plugin.datax.kafka.reader.subscriptionmethod;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.realtime.transfer.DTO;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Pattern;

/**
 * sample.topic
 */
public class KafkaSubscribeToAllTopicsMatchingSpecifiedPattern extends KafkaSubscriptionMethod {
    private static final Logger logger = LoggerFactory.getLogger(KafkaSubscribeToAllTopicsMatchingSpecifiedPattern.class);
    @FormField(ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String topicPattern;

    private Pattern parsePattern() {
        return Pattern.compile(topicPattern);
    }

//    @Override
//    public void setSubscription(KafkaConsumer<byte[], byte[]> consumer) {
//        consumer.subscribe(this.parsePattern());
//
//
//    }

    @Override
    public void setSubscription(KafkaSourceBuilder<DTO> kafkaSourceBuilder) {
        kafkaSourceBuilder.setTopicPattern(this.parsePattern());
    }


    @TISExtension
    public static class DefaultDescriptor extends Descriptor<KafkaSubscriptionMethod> {
        @Override
        public String getDisplayName() {
            return "Topics Match Specified Pattern";
        }


        @Override
        protected boolean validateAll(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {

            KafkaSubscribeToAllTopicsMatchingSpecifiedPattern specByPattern = postFormVals.newInstance();
            try {
                specByPattern.parsePattern();
            } catch (Exception e) {
                msgHandler.addFieldError(context, "topicPattern", e.getMessage());
                logger.warn(e.getMessage(), e);
                return false;
            }

            return super.validateAll(msgHandler, context, postFormVals);
        }
    }
}
