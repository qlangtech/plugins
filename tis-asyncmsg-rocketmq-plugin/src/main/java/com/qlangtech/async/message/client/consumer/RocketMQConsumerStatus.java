package com.qlangtech.async.message.client.consumer;

import com.qlangtech.tis.async.message.client.consumer.IMQConsumerStatusFactory;
import org.apache.rocketmq.common.admin.ConsumeStats;
import org.apache.rocketmq.common.admin.OffsetWrapper;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;

import java.util.Set;

/**
 * @author: baisui 百岁
 * @create: 2020-08-29 16:55
 **/
public class RocketMQConsumerStatus implements IMQConsumerStatusFactory.IMQConsumerStatus {

    private final ConsumeStats consumeStats;
    private final DefaultMQAdminExt defaultMQAdminExt;

    public RocketMQConsumerStatus(ConsumeStats consumeStats, DefaultMQAdminExt defaultMQAdminExt) {
        this.consumeStats = consumeStats;
        this.defaultMQAdminExt = defaultMQAdminExt;
    }

    @Override
    public long getTotalDiff() {
        Set<MessageQueue> mqList = consumeStats.getOffsetTable().keySet();
        long diffTotal = 0L;
        for (MessageQueue mq : mqList) {
            OffsetWrapper offsetWrapper = consumeStats.getOffsetTable().get(mq);
            long diff = offsetWrapper.getBrokerOffset() - offsetWrapper.getConsumerOffset();
            diffTotal += diff;
        }
        return diffTotal;
    }

    @Override
    public void close() {
        this.defaultMQAdminExt.shutdown();
    }
}
