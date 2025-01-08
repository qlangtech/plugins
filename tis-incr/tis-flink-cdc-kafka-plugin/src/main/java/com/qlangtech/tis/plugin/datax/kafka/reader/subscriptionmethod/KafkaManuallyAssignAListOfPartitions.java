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
import com.google.common.collect.Sets;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.realtime.transfer.DTO;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * sample.topic:0, sample.topic:1
 */
public class KafkaManuallyAssignAListOfPartitions extends KafkaSubscriptionMethod {
    // private static final Pattern PATTERN_PARTITIONS = Pattern.compile("[^:^,]+?:\\d+");
    private static final Logger logger = LoggerFactory.getLogger(KafkaManuallyAssignAListOfPartitions.class);
    @FormField(ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String topicPartitions;

    private Pair<Boolean, Set<TopicPartition>> parseTopicPartition() {
        Set<TopicPartition> partitionSet = Sets.newHashSet();
        String[] partitions = StringUtils.split(this.topicPartitions, ",");
        String[] partitionPair = null;
        for (String p : partitions) {
            partitionPair = StringUtils.split(p, ":");
            if (partitionPair == null || partitionPair.length != 2 || StringUtils.isBlank(partitionPair[0])) {
                return Pair.of(false, partitionSet);
            }
            try {
                partitionSet.add(
                        new TopicPartition(StringUtils.trim(partitionPair[0]), Integer.parseInt(partitionPair[1])));
            } catch (NumberFormatException e) {
                logger.warn(e.getMessage(), e);
                return Pair.of(false, partitionSet);
            }
        }
        return Pair.of(partitionSet.size() > 0, partitionSet);
    }

//    @Override
//    public void setSubscription(KafkaConsumer<byte[], byte[]> consumer) {
//        consumer.assign(getTopicPartitions().getRight());
//    }

    @Override
    public void setSubscription(KafkaSourceBuilder<DTO> kafkaSourceBuilder) {
        Pair<Boolean, Set<TopicPartition>> topicPartitions = getTopicPartitions();
        kafkaSourceBuilder.setPartitions(topicPartitions.getRight());
    }

    private Pair<Boolean, Set<TopicPartition>> getTopicPartitions() {
        Pair<Boolean, Set<TopicPartition>> topicPartitions = parseTopicPartition();
        if (!topicPartitions.getKey()) {
            throw new IllegalStateException("topicPartitions is invalid:" + topicPartitions);
        }
        return topicPartitions;
    }

    @TISExtension
    public static class DefaultDescriptor extends Descriptor<KafkaSubscriptionMethod> {
        @Override
        public String getDisplayName() {
            return "List Of Partitions";
        }

        @Override
        protected boolean validateAll(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {

            KafkaManuallyAssignAListOfPartitions topicPartitions = postFormVals.newInstance();
            Pair<Boolean, Set<TopicPartition>> parseResult = topicPartitions.parseTopicPartition();
            if (!parseResult.getKey()) {
                msgHandler.addFieldError(context, "topicPartitions", "不符合格式要求");
                return false;
            }

            return super.validateAll(msgHandler, context, postFormVals);
        }
    }
}
