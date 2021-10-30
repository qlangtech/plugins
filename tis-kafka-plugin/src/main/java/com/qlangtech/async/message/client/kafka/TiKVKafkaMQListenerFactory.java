/**
 * Copyright (c) 2020 QingLang, Inc. <baisui@qlangtech.com>
 * <p>
 *   This program is free software: you can use, redistribute, and/or modify
 *   it under the terms of the GNU Affero General Public License, version 3
 *   or later ("AGPL"), as published by the Free Software Foundation.
 * <p>
 *  This program is distributed in the hope that it will be useful, but WITHOUT
 *  ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 *   FITNESS FOR A PARTICULAR PURPOSE.
 * <p>
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.qlangtech.async.message.client.kafka;

import com.qlangtech.tis.async.message.client.consumer.IMQListener;
import com.qlangtech.tis.async.message.client.consumer.impl.MQListenerFactory;
import com.qlangtech.tis.datax.IDataXPluginMeta;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;

import java.util.Optional;

/**
 * @author: baisui 百岁
 * @create: 2020-12-09 11:43
 **/
public class TiKVKafkaMQListenerFactory extends MQListenerFactory {
//    public static void main(String[] args) {
//        List<KafkaMessage> kafkaMessages = Lists.newArrayList();
//        TicdcEventFilter filter = new TicdcEventFilter();
//        for (KafkaMessage kafkaMessage : kafkaMessages) {
//            parseKafkaMessage(filter, kafkaMessage);
//        }
//
//    }

//    public static final String MQ_ADDRESS_COLLECTION = "192.168.28.201:9092";			//kafka地址
//    public static final String CONSUMER_TOPIC = "baisui";						//消费者连接的topic
//    // public static final String PRODUCER_TOPIC = "topicDemo";						//生产者连接的topic
//    public static final String CONSUMER_GROUP_ID = "1";

    @FormField(ordinal = 0, validate = {Validator.require, Validator.host})
    public String mqAddress;
    @FormField(ordinal = 1, validate = {Validator.require, Validator.identity})
    public String topic;
    @FormField(ordinal = 2, validate = {Validator.require, Validator.identity})
    public String groupId;
    @FormField(ordinal = 3, type = FormFieldType.ENUM, validate = {Validator.require})
    //earliest,latest,和none
    public String offsetResetStrategy;


    @Override
    public IMQConsumerStatus createConsumerStatus() {
        return null;
    }

    @Override
    public IMQListener create() {
        KafkaMQListener mqListener = new KafkaMQListener(this);
        return mqListener;
    }

    @TISExtension()
    public static class DefaultDescriptor extends BaseDescriptor {
        @Override
        public String getDisplayName() {
            return "TiCDC-Kafka";
        }
        @Override
        public Optional<IDataXPluginMeta.EndType> getTargetType() {
            return Optional.empty();
        }
    }
}
