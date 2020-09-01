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
