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

package com.qlangtech.async.message.client.consumer;

import com.qlangtech.tis.TIS;
import com.qlangtech.tis.async.message.client.consumer.IMQConsumerStatusFactory;
import com.qlangtech.tis.async.message.client.consumer.impl.MQListenerFactory;
import com.qlangtech.tis.plugin.PluginStore;
import junit.framework.TestCase;

/**
 * @author: baisui 百岁
 * @create: 2020-08-29 17:38
 **/
public class TestRocketMQListenerFactory extends TestCase {
    private static final String collection = "search4totalpay";

    public void testCreateConsumerStatus() {

        PluginStore<MQListenerFactory> mqListenerFactory = TIS.getPluginStore(collection, MQListenerFactory.class);
        assertNotNull(mqListenerFactory);

        MQListenerFactory plugin = mqListenerFactory.getPlugin();
        assertNotNull(plugin);

        IMQConsumerStatusFactory.IMQConsumerStatus consumerStatus = plugin.createConsumerStatus();
        assertNotNull(consumerStatus);


      //  assertTrue(consumerStatus.getTotalDiff() > 0);

    }
}
