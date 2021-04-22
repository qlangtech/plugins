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

//    @Override
//    public long getTotalDiff() {
//        Set<MessageQueue> mqList = consumeStats.getOffsetTable().keySet();
//        long diffTotal = 0L;
//        long diff;
//        OffsetWrapper offsetWrapper = null;
//        for (MessageQueue mq : mqList) {
//            offsetWrapper = consumeStats.getOffsetTable().get(mq);
//            diff = (offsetWrapper.getBrokerOffset() - offsetWrapper.getConsumerOffset());
//            diffTotal += diff;
//        }
//        return diffTotal;
//    }

//    @Override
//    public void close() {
//        this.defaultMQAdminExt.shutdown();
//    }
}
