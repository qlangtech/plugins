package com.qlangtech.tis.mq;

import junit.framework.TestCase;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * @author: baisui 百岁
 * @create: 2020-08-06 14:53
 **/
public class TestConsumer extends TestCase {

    public void testConsume() throws Exception {
        // Instantiate with specified consumer group name.
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(TestTotalpayProducter.topic + "_consume1");
        // Specify name server addresses.
        consumer.setNamesrvAddr(TestTotalpayProducter.nameAddress);
        // Subscribe one more more topics to consume.
        consumer.subscribe(TestTotalpayProducter.topic, "*");
        // Register callback to execute on arrival of messages fetched from brokers.
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                            ConsumeConcurrentlyContext context) {
                System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        //Launch the consumer instance.
        consumer.start();

        System.out.printf("Consumer Started.%n");
        Thread.sleep(20 * 1000);
    }
}
