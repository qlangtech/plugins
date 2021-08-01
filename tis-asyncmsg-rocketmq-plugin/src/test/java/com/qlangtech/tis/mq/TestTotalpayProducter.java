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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.model.GetObjectRequest;
import com.aliyun.oss.model.OSSObject;
import com.google.common.collect.Maps;
import com.qlangtech.tis.manage.common.TisUTF8;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * @author: baisui 百岁
 * @create: 2020-08-06 13:21
 **/
public class TestTotalpayProducter extends BasicProducer {

    private static final Map<String, String> ossPathMap = Maps.newHashMap();

    static {
        System.setProperty(ClientLogger.CLIENT_LOG_USESLF4J, String.valueOf(Boolean.TRUE));
        ossPathMap.put("order", "totalpay/7d4c33f07948492f9ba4b4040bc905fc");
        ossPathMap.put("binlogmsg", "binlogmsg/0f1305acfe534f8cb9d730aa9b1f9a26");
    }

    static final OSS client;

    static {
        client = new OSSClientBuilder().build("http://oss-cn-hangzhou.aliyuncs.com"
                , "", "");
    }

    public void testProduce() throws Exception {
        //Instantiate with a producer group name.
        DefaultMQProducer producter = createProducter();
        System.out.println("start to send message to MQ");

        consumeFile(producter);

        //Shut down once the producer instance is not longer in use.
        producter.shutdown();
    }

//    public void testConsumeFile() throws Exception {
//        this.consumeFile();
//    }

    private void consumeFile(DefaultMQProducer producer) throws Exception {

        GetObjectRequest getObjectRequest = new GetObjectRequest("incr-log", ossPathMap.get("order"));
        // getObjectRequest.setRange(3000, -1);
        OSSObject object = client.getObject(getObjectRequest);
        LineIterator lit = null;
        Message msg = null;
        JSONObject m = null;
        String line = null;
        String tag = null;
        AtomicInteger incr = null;
        ConcurrentHashMap<String, AtomicInteger> statis = new ConcurrentHashMap();
        long lastTimestamp = 0;
        long current;
        int allcount = 0;
        try (InputStream input = object.getObjectContent()) {
            lit = IOUtils.lineIterator(input, TisUTF8.get());
            while (lit.hasNext()) {
                try {
                    line = lit.nextLine();
                    allcount++;
                    m = JSON.parseObject(line);
                    tag = m.getString("orginTableName");

                    msg = createMsg(line, tag);
                    if ((incr = statis.get(tag)) == null) {
                        incr = statis.computeIfAbsent(tag, (key) -> new AtomicInteger());
                    }
                    incr.incrementAndGet();
                    SendResult sendResult = producer.send(msg);
                    Thread.sleep(10);
                    current = System.currentTimeMillis();
                    if (current > (lastTimestamp + 5000)) {
                        System.out.println("<---------------------------");
                        System.out.println(statis.entrySet().stream().map((e) -> e.getKey() + ":" + e.getValue().get()).collect(Collectors.joining("\n")));
                        System.out.println("allcount:" + allcount);
                        lastTimestamp = current;
                        System.out.println("--------------------------->");
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                    throw new IllegalStateException("line:" + line, e);
                }
            }
        }


//        int count = 0;hh
//        LineIterator lit = FileUtils.lineIterator(new File("/Users/mozhenghua/Downloads/s4shop"), TisUTF8.getName());
//        JSONObject msg = null;
//        JSONObject before = null;
//        JSONObject after = null;
//        while (lit.hasNext()) {
//            count++;
//            msg = JSON.parseObject(lit.nextLine());
//            before = msg.getJSONObject("before");
//            after = msg.getJSONObject("after");
//            msg.getString("dbName");
//            msg.getString("eventType");
//            msg.getString("orginTableName");
//            msg.getString("targetTable");
//        }
//        System.out.println("count:" + count);
    }


}
