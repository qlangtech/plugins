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

package com.qlangtech.tis.mq;

import com.qlangtech.tis.manage.common.TisUTF8;
import junit.framework.TestCase;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;

/**
 * @author: baisui 百岁
 * @create: 2020-11-02 10:13
 **/
public class BasicProducer extends TestCase {
    public static final String nameAddress = "192.168.28.201:9876";
    public static final String topic = "baisui-test";

    protected Message createMsg(String line, String tag) {
        return new Message(topic /* Topic */, tag, line.getBytes(TisUTF8.get()) /* Message body */);
    }

    protected DefaultMQProducer createProducter() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("produce_baisui_test");
        // https://github.com/apache/rocketmq/issues/568
        producer.setVipChannelEnabled(false);
        producer.setSendMsgTimeout(30000);
        // Specify name server addresses.
        producer.setNamesrvAddr(nameAddress);
        //Launch the instance.
        producer.start();
        return producer;
    }

}
