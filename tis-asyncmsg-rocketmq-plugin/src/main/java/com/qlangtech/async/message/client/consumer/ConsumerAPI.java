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
import com.alibaba.rocketmq.client.impl.consumer.ProcessQueue;
import com.alibaba.rocketmq.common.message.MessageQueue;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/*
 * Created with IntelliJ IDEA.
 * User:jiandan
 * Date:2016/7/4.
 * Time:13:40.
 * INFO:Consumer 的 api 接口
 * 1、需要注入
 * 2、
 *
 * @author 百岁（baisui@qlangtech.com）
 * @date 2020/04/13
 */
@Deprecated
public class ConsumerAPI {

    // 消费者
    private DefaultMQPushConsumer consumer;

    /**
     * 估摸着算出当前消费者堆积的消息数量，因为这个数量不是非常准确的
     *
     * @return
     */
    public long computeAccumulationTotal() {
        long msgAccTotal = 0;
        ConcurrentHashMap<MessageQueue, ProcessQueue> processQueueTable = consumer.getDefaultMQPushConsumerImpl().getRebalanceImpl().getProcessQueueTable();
        Iterator<Map.Entry<MessageQueue, ProcessQueue>> it = processQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<MessageQueue, ProcessQueue> next = it.next();
            ProcessQueue value = next.getValue();
        // msgAccTotal += value.getMsgDuijiCnt(); // 当前的消息总量，在客户端这边，是通过 offset 来计算的
        }
        return msgAccTotal;
    }

    public void setConsumer(DefaultMQPushConsumer consumer) {
        this.consumer = consumer;
    }
}
