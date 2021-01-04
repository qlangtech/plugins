package com.qlangtech.async.message.client.kafka;

import com.alibaba.fastjson.JSON;
import com.qlangtech.tis.async.message.client.consumer.AsyncMsg;
import com.qlangtech.tis.async.message.client.consumer.IConsumerHandle;
import com.qlangtech.tis.async.message.client.consumer.IMQListener;
import junit.framework.TestCase;

import java.io.IOException;

/**
 * @author: baisui 百岁
 * @create: 2020-12-09 15:01
 **/
public class TestKafkaMQListener extends TestCase {
    public void testRegisterTopic() throws Exception {

        TiKVKafkaMQListenerFactory listenerFactory = new TiKVKafkaMQListenerFactory();
        listenerFactory.topic = "baisui";
        listenerFactory.groupId = "test1";
        listenerFactory.mqAddress = "192.168.28.201:9092";
        listenerFactory.offsetResetStrategy = "latest";


        IMQListener imqListener = listenerFactory.create();
        imqListener.setConsumerHandle(new MockConsumer());
        imqListener.start();

        Thread.sleep(99999999);

    }

    private static class MockConsumer implements IConsumerHandle {
        @Override
        public String getSubExpression() {
            return "employees";
        }

        @Override
        public boolean consume(AsyncMsg asyncMsg) {
            try {
                System.out.println(JSON.toJSONString(asyncMsg.getContent()));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return true;
        }
    }
}
