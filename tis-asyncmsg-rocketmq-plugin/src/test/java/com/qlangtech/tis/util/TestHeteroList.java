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
package com.qlangtech.tis.util;

import com.google.common.collect.Lists;
import com.qlangtech.tis.async.message.client.consumer.impl.MQListenerFactory;
import junit.framework.TestCase;

import java.util.List;

/*
 * @create: 2020-02-08 16:23
 *
 * @author 百岁（baisui@qlangtech.com）
 * @date 2020/04/13
 */
public class TestHeteroList extends TestCase {

    private static final String caption = "test-caption";

    public void testReflectAllMethod() {
        // HeteroList<AbstractMQListenerFactory> hList = new HeteroList<>();
        List<MQListenerFactory> items = Lists.newArrayList();
        // 
        // final Type col = Types.getBaseClass(items.getClass(), List.class);
        // System.out.println(col);
        // System.out.println(items.getClass().getComponentType());
        HeteroList<MQListenerFactory> heteroList = HeteroList.getHeteroList(caption, items, MQListenerFactory.class);
        assertEquals(caption, heteroList.getCaption());
        // assertEquals(1, heteroList.getDescriptors().size());
        assertEquals(0, heteroList.getItems().size());
    }
}
