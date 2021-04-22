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
package com.qlangtech.tis.component;

import com.qlangtech.async.message.client.consumer.RocketMQListenerFactory;
import com.qlangtech.async.message.client.to.impl.DefaultJSONFormatDeserialize;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.async.message.client.consumer.impl.MQListenerFactory;
import com.qlangtech.tis.plugin.PluginStore;

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
       // IncrComponent incrComponent = TIS.get().loadIncrComponent(collection);

        PluginStore<MQListenerFactory> pluginStore = TIS.getPluginStore(collection, MQListenerFactory.class);

      //  assertNotNull(incrComponent);
        List<MQListenerFactory> mqListenerFactoryList = pluginStore.getPlugins();// incrComponent.getMqListenerFactory();
        assertEquals(1, mqListenerFactoryList.size());
        MQListenerFactory mqListenerFactory = mqListenerFactoryList.stream().findFirst().get();
        assertTrue(mqListenerFactory instanceof RocketMQListenerFactory);
        RocketMQListenerFactory rmFactory = (RocketMQListenerFactory) mqListenerFactory;
        assertEquals("c_otter_binlogorder_solr", rmFactory.getConsumeName());
        assertEquals("otter_binlogorder", rmFactory.getMqTopic());
        assertEquals("10.1.21.148:9876", rmFactory.getNamesrvAddr());
        assertTrue(rmFactory.getDeserialize() instanceof DefaultJSONFormatDeserialize);
      //  assertEquals(collection, incrComponent.getCollection());
    }
}
