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
import org.apache.commons.lang.StringUtils;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-12-27 15:27
 **/
public class KafkaListOfTopics extends KafkaSubscriptionMethod {
    @FormField(ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String topics;

    private String[] parseTopics() {
        return StringUtils.split(topics, ",");
    }

    @Override
    public void setSubscription(KafkaSourceBuilder<DTO> kafkaSourceBuilder) {
        kafkaSourceBuilder.setTopics(parseTopics());
    }

//    @Override
//    public void setSubscription(KafkaConsumer<byte[], byte[]> consumer) {
//        consumer.subscribe(Lists.newArrayList(this.parseTopics()));
//    }

    @TISExtension
    public static class DefaultDescriptor extends Descriptor<KafkaSubscriptionMethod> {
        @Override
        public String getDisplayName() {
            return "List Of Topics";
        }

        @Override
        protected boolean validateAll(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
            KafkaListOfTopics topics = postFormVals.newInstance();
            String[] parseResult = topics.parseTopics();
            if (parseResult == null || parseResult.length < 1) {
                msgHandler.addFieldError(context, "topics", "不符合格式要求");
                return false;
            }
            return super.validateAll(msgHandler, context, postFormVals);
        }
    }
}
