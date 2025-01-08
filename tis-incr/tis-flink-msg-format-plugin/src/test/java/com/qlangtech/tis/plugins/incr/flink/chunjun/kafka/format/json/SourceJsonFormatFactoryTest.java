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

package com.qlangtech.tis.plugins.incr.flink.chunjun.kafka.format.json;

import com.qlangtech.tis.manage.common.TisUTF8;
import com.qlangtech.tis.plugin.kafka.consumer.KafkaStructuredRecord;
import com.qlangtech.tis.realtime.transfer.DTO.EventType;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Map;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-01-05 07:48
 **/
public class SourceJsonFormatFactoryTest {

    @Test
    public void parseRecord() {

        String data = "{\n" +
                "  \t\t\"id\": \"99933194ffe98b9e407c4b61a67962da\",\n" +
                "  \t\t\"price\": 12.56\n" +
                "         }";

        SourceJsonFormatFactory formatFactory = new SourceJsonFormatFactory();
        KafkaStructuredRecord reuse = new KafkaStructuredRecord();
        KafkaStructuredRecord record = formatFactory.parseRecord(reuse, data.getBytes(TisUTF8.get()));
        Assert.assertNotNull(record);
        Assert.assertEquals(EventType.ADD, record.getEventType());
        Map<String, Object> afterVals = record.vals;
        Assert.assertNotNull(afterVals);

        Assert.assertEquals(2, afterVals.size());
        Assert.assertEquals("99933194ffe98b9e407c4b61a67962da", afterVals.get("id"));
        Assert.assertEquals(BigDecimal.valueOf(1256, 2), afterVals.get("price"));

        Assert.assertNull("before vals must be empty", record.getOldVals());

    }

    @Test
    public void validateFormtField() {
    }
}