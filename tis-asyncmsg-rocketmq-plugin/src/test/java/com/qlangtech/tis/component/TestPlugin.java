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

import com.alibaba.fastjson.JSONArray;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.async.message.client.consumer.impl.AbstractMQListenerFactory;
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.util.HeteroList;
import com.qlangtech.tis.manage.common.TisUTF8;
import com.qlangtech.async.message.client.consumer.RocketMQListenerFactory;
import com.qlangtech.async.message.client.to.impl.HessianDeserialize;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.json.JSONObject;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/*
 * @create: 2020-01-14 09:21
 *
 * @author 百岁（baisui@qlangtech.com）
 * @date 2020/04/13
 */
public class TestPlugin extends BaseTestCase {

    // public void testGetExtensionList() {
    // ExtensionList<ExtensionFinder> extensionList = TIS.get().getExtensionList(ExtensionFinder.class);
    // assertEquals(1, extensionList.size());
    // }
    private static final String consumeId = ("c_otter_binlogorder_solr");

    // listener.setDeserialize(new HessianDeserialize());
    private static final String MQ_TOPIC = ("otter_binlogorder");

    private static final String NamesrvAddr = ("10.1.21.148:9876");

    private static final String collection = "search4totalpay";

    private static final File tmpDir = new File("/tmp/opt/data");

    static {
        System.setProperty("data.dir", tmpDir.getAbsolutePath());
    }

    public static class Person {

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }

        private int age;
    }

    // public void testSetVal() throws Exception {
    // 
    // Person p = new Person();
    // 
    // BeanUtils.setProperty(p, "age", "12");
    // 
    // System.out.println(p.getAge());
    // 
    // ConvertUtilsBean convertUtils = new ConvertUtilsBean();
    // 
    // System.out.println(convertUtils.convert("99887", Integer.class));
    // }
    public void testReceiceRequestFromClient() throws Exception {
        com.alibaba.fastjson.JSONArray jsonArray = null;
        com.alibaba.fastjson.JSONObject jsonObject = null;
        com.alibaba.fastjson.JSONObject valJ = null;
        String impl = null;
        Descriptor.PropertyType attrDesc = null;
        Descriptor descriptor = null;
        JSONArray vals = null;
        String attr = null;
        String attrVal = null;
        TIS tis = TIS.get();
        IncrComponent incrComponent = tis.loadIncrComponent(collection);
        // incrComponent.setMqListenerFactory();
        Describable describable = null;
        try (InputStream input = TestPlugin.class.getResourceAsStream("RocketMQListenerFactory.json")) {
            assertNotNull(input);
            jsonArray = JSONArray.parseArray(IOUtils.toString(input, TisUTF8.getName()));
            for (int i = 0; i < jsonArray.size(); i++) {
                // 创建一个item
                jsonObject = jsonArray.getJSONObject(i);
                describable = parseDescribable(jsonObject).instance;
            }
        }
        assertNotNull(describable);
        RocketMQListenerFactory mqListenerFactory = (RocketMQListenerFactory) describable;
        assertEquals(MQ_TOPIC, mqListenerFactory.getMqTopic());
        assertEquals(NamesrvAddr, mqListenerFactory.getNamesrvAddr());
        assertEquals(consumeId, mqListenerFactory.consumeName);
        assertNotNull(mqListenerFactory.getDeserialize());
    }

    private Descriptor.ParseDescribable parseDescribable(com.alibaba.fastjson.JSONObject jsonObject) {
        String impl;
        JSONArray vals;
        Descriptor descriptor;
        String attr;
        Descriptor.PropertyType attrDesc;
        com.alibaba.fastjson.JSONObject valJ;
        String attrVal;
        impl = jsonObject.getString("impl");
        vals = jsonObject.getJSONArray("vals");
        descriptor = TIS.get().getDescriptor(impl);
        assertNotNull("impl:" + impl, descriptor);
        Descriptor.ParseDescribable describable = descriptor.newInstance(Descriptor.parseAttrValMap(jsonObject.getJSONArray("vals")));
        return describable;
    }

    public void testSaveAndLoad() throws IOException {
        FileUtils.forceMkdir(tmpDir);
        try {
            IncrComponent incrComponent = createIncrComponent();
            TIS.get().saveComponent(collection, incrComponent);
            incrComponent = TIS.get().loadIncrComponent(collection);
            List<AbstractMQListenerFactory> mqListenerFactory = incrComponent.getMqListenerFactory();
            assertEquals(1, mqListenerFactory.size());
            RocketMQListenerFactory rocketMQListenerFactory = (RocketMQListenerFactory) mqListenerFactory.get(0);
            assertEquals(collection, incrComponent.getCollection());
            assertEquals(consumeId, rocketMQListenerFactory.consumeName);
            assertEquals(MQ_TOPIC, rocketMQListenerFactory.getMqTopic());
            assertEquals(NamesrvAddr, rocketMQListenerFactory.getNamesrvAddr());
            assertNotNull(rocketMQListenerFactory.getDeserialize());
            assertTrue(rocketMQListenerFactory.getDeserialize() instanceof HessianDeserialize);
        } finally {
        // FileUtils.forceDelete(tmpDir);
        }
    }

    public void testSerialize() throws Exception {
        IncrComponent incrComponent = createIncrComponent();
        List<AbstractMQListenerFactory> mqListenerFactory = incrComponent.getMqListenerFactory();
        HeteroList<AbstractMQListenerFactory> hList = new HeteroList<>();
        hList.setCaption("MQ消息监听");
        hList.setItems(mqListenerFactory);
        hList.setDescriptors(TIS.getPluginStore(AbstractMQListenerFactory.class).allDescriptor());
        assertEquals(1, hList.getDescriptors().size());
        Map<String, Descriptor.PropertyType> propertyTypes;
        for (Descriptor<AbstractMQListenerFactory> f : hList.getDescriptors()) {
            System.out.println(f.getId());
            propertyTypes = f.getPropertyTypes();
            for (Map.Entry<String, Descriptor.PropertyType> entry : propertyTypes.entrySet()) {
                System.out.println(entry.getKey() + ":" + entry.getValue());
            }
        }
        JSONObject j = hList.toJSON();
        System.out.println("==============================");
        System.out.println(j.toString(1));
        System.out.println("==============================");
    }

    private IncrComponent createIncrComponent() {
        IncrComponent incrComponent = new IncrComponent("search4totalpay");
        List<AbstractMQListenerFactory> mqListenerFactory = new ArrayList<>();
        RocketMQListenerFactory listener = new RocketMQListenerFactory();
        listener.setConsumeName(consumeId);
        listener.setDeserialize(new HessianDeserialize());
        listener.setMqTopic(MQ_TOPIC);
        listener.setNamesrvAddr(NamesrvAddr);
        // assertNotNull("consumeHandle can not null", listener.getConsumeHandle());
        Descriptor.PropertyType deserializeProptype = listener.getDescriptor().getPropertyType("deserialize");
        assertNotNull("deserializeProptype can not be null", deserializeProptype);
        List<? extends Descriptor> applicableDescriptors = deserializeProptype.getApplicableDescriptors();
        assertTrue("applicableDescriptors size:" + applicableDescriptors.size(), applicableDescriptors.size() > 0);
        mqListenerFactory.add(listener);
        incrComponent.setMqListenerFactory(mqListenerFactory);
        return incrComponent;
    }
}
