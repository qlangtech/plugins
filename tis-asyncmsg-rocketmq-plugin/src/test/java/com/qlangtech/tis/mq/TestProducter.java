package com.qlangtech.tis.mq;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.model.OSSObject;
import com.google.common.collect.Maps;
import com.qlangtech.tis.manage.common.TisUTF8;
import junit.framework.TestCase;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;

import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * @author: baisui 百岁
 * @create: 2020-08-06 13:21
 **/
public class TestProducter extends TestCase {

    public static final String nameAddress = "192.168.28.201:9876";
    public static final String topic = "baisui-test";

    private static final Map<String, String> ossPathMap = Maps.newHashMap();

    static {
        ossPathMap.put("order", "totalpay/7d4c33f07948492f9ba4b4040bc905fc");
        ossPathMap.put("binlogmsg", "binlogmsg/0f1305acfe534f8cb9d730aa9b1f9a26");
    }

    static final OSS client;

    static {
        client = new OSSClientBuilder().build("http://oss-cn-hangzhou.aliyuncs.com"
                , "LTAICjPfS8Y2fDkN", "0xkKmOFLLbvrbM6gknWbbw2EDSabdR");
    }

    public void testProduce() throws Exception {
        //Instantiate with a producer group name.
        DefaultMQProducer producer = new DefaultMQProducer("produce_baisui_test");
        // Specify name server addresses.
        producer.setNamesrvAddr(nameAddress);
        //Launch the instance.
        producer.start();
//        for (int i = 0; i < 100; i++) {
//            //Create a message instance, specifying topic, tag and message body.
//            Message msg = new Message(topic /* Topic */,
//                    "TagA" /* Tag */,
//                    ("Hello RocketMQ " +
//                            i).getBytes(TisUTF8.get()) /* Message body */
//            );
//            //Call send message to deliver message to one of brokers.
//            SendResult sendResult = producer.send(msg);
//            System.out.printf("%s%n", sendResult);
//        }

        consumeFile(producer);

        //Shut down once the producer instance is not longer in use.
        producer.shutdown();
    }

//    public void testConsumeFile() throws Exception {
//        this.consumeFile();
//    }

    private void consumeFile(DefaultMQProducer producer) throws Exception {

        OSSObject object = client.getObject("incr-log", ossPathMap.get("order"));
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

                    msg = new Message(topic /* Topic */, tag, line.getBytes(TisUTF8.get()) /* Message body */);
                    if ((incr = statis.get(tag)) == null) {
                        incr = statis.computeIfAbsent(tag, (key) -> new AtomicInteger());
                    }
                    incr.incrementAndGet();
                    SendResult sendResult = producer.send(msg);
                    Thread.sleep(100);
                    current = System.currentTimeMillis();
                    if (current > (lastTimestamp + 5000)) {
                        System.out.println("<---------------------------");
                        System.out.println(statis.entrySet().stream().map((e) -> e.getKey() + ":" + e.getValue().get()).collect(Collectors.joining("\n")));
                        System.out.println("allcount:" + allcount);
                        lastTimestamp = current;
                        System.out.println("--------------------------->");
                    }

                } catch (Exception e) {
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
