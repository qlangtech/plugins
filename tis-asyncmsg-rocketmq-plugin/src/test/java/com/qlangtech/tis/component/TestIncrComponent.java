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
package com.qlangtech.tis.component;

import com.qlangtech.async.message.client.consumer.RocketMQListenerFactory;
import com.qlangtech.async.message.client.to.impl.HessianDeserialize;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.async.message.client.consumer.impl.MQListenerFactory;

import java.util.List;

/*
 * @create: 2020-02-05 11:14
 *
 * @author 百岁（baisui@qlangtech.com）
 * @date 2020/04/13
 */
public class TestIncrComponent extends BaseTestCase {

    private static final String collection = "search4totalpay";

    public void testLoad() {
        IncrComponent incrComponent = TIS.get().loadIncrComponent(collection);
        assertNotNull(incrComponent);
        List<MQListenerFactory> mqListenerFactoryList = incrComponent.getMqListenerFactory();
        assertEquals(1, mqListenerFactoryList.size());
        MQListenerFactory mqListenerFactory = mqListenerFactoryList.stream().findFirst().get();
        assertTrue(mqListenerFactory instanceof RocketMQListenerFactory);
        RocketMQListenerFactory rmFactory = (RocketMQListenerFactory) mqListenerFactory;
        assertEquals("c_otter_binlogorder_solr", rmFactory.getConsumeName());
        assertEquals("otter_binlogorder", rmFactory.getMqTopic());
        assertEquals("10.1.21.148:9876", rmFactory.getNamesrvAddr());
        assertTrue(rmFactory.getDeserialize() instanceof HessianDeserialize);
        assertEquals(collection, incrComponent.getCollection());
    }
}
