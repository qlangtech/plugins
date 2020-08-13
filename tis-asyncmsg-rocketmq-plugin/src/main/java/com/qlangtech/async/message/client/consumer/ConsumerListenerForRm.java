/* * Copyright 2020 QingLang, Inc.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.qlangtech.async.message.client.consumer;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.*;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;
import com.qlangtech.async.message.client.to.impl.AsyncMsgRM;
import com.qlangtech.tis.async.message.client.consumer.IAsyncMsgDeserialize;
import com.qlangtech.tis.async.message.client.consumer.MQConsumeException;
import com.qlangtech.async.message.client.util.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import java.net.InetAddress;
import java.util.List;
import static com.qlangtech.async.message.client.util.MD5Util.stringIsEmpty;

/* *
 * @author 百岁（baisui@qlangtech.com）
 * @date 2020/04/13
 */
public class ConsumerListenerForRm extends BaseConsumerListener implements InitializingBean {

    private Logger logger = LoggerFactory.getLogger(ConsumerListenerForRm.class);

    // 消费者
    private DefaultMQPushConsumer consumer;

    // 改消费者所属的组
    private String consumerGroup;

    // RocketMQ 的 NameSRV
    private String namesrvAddr;

    // 消费模式。默认是集群模式
    private MessageModel messageModel = MessageModel.CLUSTERING;

    // 一个新的订阅组默认第一次启动从队列的最后位置开始消费
    private ConsumeFromWhere consumeFromWhere = ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET;

    // 暂停消费，日常环境可用（可在控制台配置消费者的启停）
    public static volatile boolean suspend = false;

    // 回溯消费时间
    private String consumeTimestamp;

    // 是否启动顺序消费组
    private RegisterConsumeType registerConsumeType = RegisterConsumeType.CONCURRENTLY;

    // 消费消息线程，最小数目
    private int consumeThreadMin = 20;

    private IAsyncMsgDeserialize deserialize;

    public IAsyncMsgDeserialize getDeserialize() {
        return deserialize;
    }

    public void setDeserialize(IAsyncMsgDeserialize deserialize) {
        this.deserialize = deserialize;
    }

    /**
     * 启动，需要在bean中初始化
     */
    public void start() throws MQConsumeException {
        try {
            /**
             * 一个应用创建一个Consumer，由应用来维护此对象，可以设置为全局对象或者单例<br>
             * 注意：ConsumerGroupName需要由应用来保证唯一
             */
            // Assert.isTrue(consumerGroup.startsWith("c_") && consumerGroup.contains(topic), "消费者不符合规范！consumerGroup:" + consumerGroup + ",topic:" + topic);
            // 广播模式采用动态消费组的方式
            if (messageModel.equals(MessageModel.BROADCASTING)) {
                consumerGroup = consumerGroup + InetAddress.getLocalHost().getHostAddress().replace(".", "_");
            }
            if (// MessageConfig.checkUnPublishEnv() &&
            suspend)
                return;
            consumer = new DefaultMQPushConsumer(consumerGroup);
            consumer.setNamesrvAddr(namesrvAddr);
            consumer.setConsumeThreadMin(consumeThreadMin);
            // 订阅指定topic下tags
            consumer.subscribe(topic, consumerHandle.getSubExpression());
            /**
             * Consumer第一次启动默认从队列尾部开始消费
             * 如果非第一次启动，那么按照上次消费的位置继续消费
             */
            consumer.setConsumeFromWhere(consumeFromWhere);
            if (consumeFromWhere.equals(ConsumeFromWhere.CONSUME_FROM_TIMESTAMP) && !stringIsEmpty(consumeTimestamp)) {
                consumer.setConsumeTimestamp(consumeTimestamp);
            }
            switch(registerConsumeType) {
                case ORDERLY:
                    consumer.registerMessageListener(new BaseListenerOrderly());
                    break;
                case CONCURRENTLY:
                default:
                    consumer.registerMessageListener(new BaseListenerConcurrently());
                    break;
            }
            consumer.start();
            logger.info("ConsumerListenerForRm started!topic:" + topic + ",expression:" + consumerHandle.getSubExpression()
                    + "  consumerGroup:" + consumerGroup + "   namesrvAddr:" + namesrvAddr);
        } catch (Exception e) {
            throw new MQConsumeException(e.getMessage(), e);
        }
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public void setNamesrvAddr(String namesrvAddr) {
        this.namesrvAddr = namesrvAddr;
    }

    public MessageModel getMessageModel() {
        return messageModel;
    }

    /**
     * 设置消息模型：集群模式/广播模式
     */
    public void setMessageModel(MessageModel messageModel) {
        this.messageModel = messageModel;
    }

    /**
     * 一个新的订阅组第一次启动时，从队列的什么位置开始消费（默认从队列尾部开始消费）
     *
     * @param consumeFromWhere
     */
    public void setConsumeFromWhere(ConsumeFromWhere consumeFromWhere) {
        this.consumeFromWhere = consumeFromWhere;
    }

    /**
     * 当设置Consumer第一次启动为回溯消费时，回溯到哪个时间点.
     *
     * @param consumeTimestamp 数据格式如下，时间精度秒:20131223171201，表示2013年12月23日17点12分01秒
     */
    public void setConsumeTimestamp(String consumeTimestamp) {
        this.consumeTimestamp = consumeTimestamp;
    }

    /**
     * 注册的消息监听器类型（默认为CONCURRENTLY）
     *
     * @param registerConsumeType
     */
    public void setRegisterConsumeType(RegisterConsumeType registerConsumeType) {
        this.registerConsumeType = registerConsumeType;
    }

    public enum RegisterConsumeType {

        ORDERLY, CONCURRENTLY
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        this.start();
    }

    /**
     * 消费消息线程数目
     */
    public void setConsumeThreadMin(int consumeThreadMin) {
        this.consumeThreadMin = consumeThreadMin;
    }

    public void close() {
        this.consumer.shutdown();
        logger.info("ConsumerListenerForRm closed !");
    }

    private class BaseListenerConcurrently implements MessageListenerConcurrently {

        @Override
        public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
            MessageExt msg = msgs.get(0);
            AsyncMsgRM asyncMsgRM = new AsyncMsgRM(msg, deserialize);
            // ActiveSpan activeSpan = buildSpan(msg);
            boolean ret = consumerHandle.consume(asyncMsgRM);
            if (!ret) {
                logger.error("consume MQ failed,msgID:" + msg.getMsgId());
            } else {
                logger.debug("consume MQ,msg:" + msg.toString() + ",handle result:success.");
            }
            // }
            return ret ? ConsumeConcurrentlyStatus.CONSUME_SUCCESS : ConsumeConcurrentlyStatus.RECONSUME_LATER;
        }
    }

    private class BaseListenerOrderly implements MessageListenerOrderly {

        @Override
        public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
            MessageExt msg = msgs.get(0);
            AsyncMsgRM asyncMsgRM = new AsyncMsgRM(msg, deserialize);
            // ActiveSpan activeSpan = buildSpan(msg);
            boolean ret = consumerHandle.consume(asyncMsgRM);
            if (!ret) {
                logger.error("consume MQ failed,msgID:" + msg.getMsgId());
            }
            // }
            return ret ? ConsumeOrderlyStatus.SUCCESS : ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
        }
    }
    // private ActiveSpan buildSpan(MessageExt msg) {
    // return null;
    // Tracer tracer = Tracing.current().tracer();
    // Tracer.SpanBuilder spanBuilder = tracer.buildSpan("consumeMsg");
    // if (msg.getProperties() != null) {
    // SpanContext spanContext = tracer.extract(Format.Builtin.TEXT_MAP, new TextMapExtractAdapter(msg.getProperties()));
    // if (spanContext != null) {
    // spanBuilder.asChildOf(spanContext);
    // }
    // }
    // ActiveSpan span = spanBuilder.startActive();
    // Tags.COMPONENT.set(span, TracerConfig.MQ_COMPONENT);
    // span.setTag(com.dfire.magiceye.Constants.REMOTE_HOST, msg.getBornHostString());
    // span.setTag(TracerConfig.TOPIC, this.topic);
    // span.setTag(Constants.SERVICE_PATH, this.topic);
    // span.setTag(TracerConfig.CONSUMER_GROUP, this.consumerGroup);
    // span.setTag(TracerConfig.CONSUME_FROM_WHERE, this.consumeFromWhere.toString());
    // span.setTag(TracerConfig.MESSAGE_MODEL, this.messageModel.toString());
    // span.setTag(TracerConfig.TAG, this.consumerHandle.getSubExpression());
    // span.setTag(Constants.BUSINESS_ID, msg.getMsgId());
    // Tags.SPAN_KIND.set(span, Tags.SPAN_KIND_CLIENT);
    // return span;
    // }
}
