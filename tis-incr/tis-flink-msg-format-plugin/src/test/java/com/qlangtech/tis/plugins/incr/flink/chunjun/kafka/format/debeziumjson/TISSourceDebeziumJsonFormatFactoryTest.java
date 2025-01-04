/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.qlangtech.tis.plugins.incr.flink.chunjun.kafka.format.debeziumjson;

import com.qlangtech.tis.manage.common.TisUTF8;
import com.qlangtech.tis.plugin.kafka.consumer.KafkaStructuredRecord;
import com.qlangtech.tis.realtime.transfer.DTO.EventType;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-01-04 22:09
 **/
public class TISSourceDebeziumJsonFormatFactoryTest {
    private static final String TEST_TABLE_NAME = "orderdetail";

    @Test
    public void parseRecord() {

        String data = "  {\n" +
                "  \"before\": null,\n" +
                "  \"after\": {\n" +
                "  \t\"order_id\": \"999331946a871d3a016a8aecf4c00197\",\n" +
                "  \t\"price\": 12.56\n" +
                "   },\n" +
                "  \"op\": \"c\",\n" +
                "  \"source\": {\n" +
                "  \t\t\"db\": \"order2\",\n" +
                "  \t\t\"table\": \"" + TEST_TABLE_NAME + "\"\n" +
                "  }\n" +
                " }";

        TISSourceDebeziumJsonFormatFactory debeziumJsonFormatFactory = new TISSourceDebeziumJsonFormatFactory();
        final KafkaStructuredRecord reuse = new KafkaStructuredRecord();
        KafkaStructuredRecord record = debeziumJsonFormatFactory.parseRecord(reuse, data.getBytes(TisUTF8.get()));
        Assert.assertNotNull(record);
        Assert.assertEquals(EventType.ADD, record.getEventType());
        Assert.assertEquals(TEST_TABLE_NAME, record.tabName);
        Map<String, Object> afterVals = record.vals;
        Assert.assertNotNull(afterVals);
        Assert.assertEquals(2, afterVals.size());
        Assert.assertEquals("999331946a871d3a016a8aecf4c00197", afterVals.get("order_id"));
        Assert.assertEquals(BigDecimal.valueOf(1256, 2), (BigDecimal) afterVals.get("price"));
        Assert.assertNull("before vals must be null", record.getOldVals());


        data = " {\n" +
                "  \"after\": {\n" +
                "  \t\"order_id\": \"9999999999999999999999999\",\n" +
                "  \t\"price\": 12.56\n" +
                "   },\n" +
                "  \"before\": {\n" +
                "  \t\"order_id\": \"666666666666666666666666\",\n" +
                "  \t\"price\": 11.11\n" +
                "   },\n" +
                "  \"op\": \"u\",\n" +
                "  \"source\": {\n" +
                "  \t\t\"db\": \"order2\",\n" +
                "  \t\t\"table\": \"orderdetail\"\n" +
                "  }\n" +
                " }";

        record = debeziumJsonFormatFactory.parseRecord(reuse, data.getBytes(TisUTF8.get()));
        Assert.assertNotNull(record);
        Assert.assertEquals(EventType.UPDATE_AFTER, record.getEventType());
        Assert.assertEquals(TEST_TABLE_NAME, record.tabName);
        afterVals = record.vals;
        Assert.assertNotNull(afterVals);
        Assert.assertEquals(2, afterVals.size());
        Assert.assertEquals("9999999999999999999999999", afterVals.get("order_id"));
        Assert.assertEquals(BigDecimal.valueOf(1256, 2), (BigDecimal) afterVals.get("price"));

        Map<String, Object> beforeVals = record.getOldVals();
        Assert.assertNotNull("before vals must be null", beforeVals);

        Assert.assertEquals(2, afterVals.size());
        Assert.assertEquals("666666666666666666666666", beforeVals.get("order_id"));
        Assert.assertEquals(BigDecimal.valueOf(1111, 2), (BigDecimal) beforeVals.get("price"));

    }
}