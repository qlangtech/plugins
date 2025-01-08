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

package com.qlangtech.tis.plugins.incr.flink.chunjun.kafka.format.canaljson;

import com.qlangtech.tis.manage.common.TisUTF8;
import com.qlangtech.tis.plugin.kafka.consumer.KafkaStructuredRecord;
import com.qlangtech.tis.realtime.transfer.DTO.EventType;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Map;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-01-04 15:09
 **/
public class TISCanalJsonFormatFactoryTest {

    private final String TEST_TABLE_NAME = "order";

    @Test
    public void testCreateDecodingFormat() throws Exception {
        TISCanalJsonFormatFactory formatFactory = new TISCanalJsonFormatFactory();
        /**
         * test ADD
         */
        String data = " {\n" +
                "  \t\"data\": [{\n" +
                "  \t\t\"id\": \"99933194ffe98b9e407c4b61a67962da\",\n" +
                "  \t\t\"price\": 12.56\n" +
                "         }],\n" +
                "  \t\"type\": \"INSERT\",\n" +
                "  \t\"table\": \"" + TEST_TABLE_NAME + "\",\n" +
                "  \t\"ts\": 1735375140702\n" +
                " }";

        KafkaStructuredRecord structuredRecord = new KafkaStructuredRecord();
        KafkaStructuredRecord record = formatFactory.parseRecord(structuredRecord, data.getBytes(TisUTF8.get()));
        Assert.assertNotNull(record);
        Assert.assertEquals(EventType.ADD, record.getEventType());
        Assert.assertEquals(TEST_TABLE_NAME, record.tabName);
        Map<String, Object> afterVals = record.vals;
        Assert.assertNotNull(afterVals);
        Assert.assertEquals(2, afterVals.size());
        Assert.assertEquals("99933194ffe98b9e407c4b61a67962da", afterVals.get("id"));
        Assert.assertEquals(BigDecimal.valueOf(1256, 2), (BigDecimal) afterVals.get("price"));

        /**
         * test UPDATE
         */
        data = "  {\n" +
                "  \t\"data\": [{\n" +
                "  \t\t\"id\": \"99933194ffe98b9e407c4b61a67962da\",\n" +
                "  \t\t\"price\": 12.56\n" +
                "         }],\n" +
                "   \"old\": [\n" +
                "      {\n" +
                "       \"id\": \"99999999999999999999999999\",\n" +
                "  \t\t\"price\": 11.99\n" +
                "      }\n" +
                "    ],     \n" +
                "  \t\"type\": \"UPDATE\",\n" +
                "  \t\"table\": \"order\",\n" +
                "  \t\"ts\": 1735375140701\n" +
                " }";

        record = formatFactory.parseRecord(structuredRecord, data.getBytes(TisUTF8.get()));
        Assert.assertNotNull(record);
        Assert.assertEquals(EventType.UPDATE_AFTER, record.getEventType());
        Assert.assertEquals(TEST_TABLE_NAME, record.tabName);
        afterVals = record.vals;
        Assert.assertNotNull(afterVals);
        Assert.assertEquals(2, afterVals.size());
        Assert.assertEquals("99933194ffe98b9e407c4b61a67962da", afterVals.get("id"));
        Assert.assertEquals(BigDecimal.valueOf(1256, 2), (BigDecimal) afterVals.get("price"));

        Map<String, Object> beforeVals = record.getOldVals();
        Assert.assertNotNull(beforeVals);
        Assert.assertEquals(2, beforeVals.size());
        Assert.assertEquals("99999999999999999999999999", beforeVals.get("id"));
        Assert.assertEquals(BigDecimal.valueOf(1199, 2), (BigDecimal) beforeVals.get("price"));

        /**
         * test DELETE
         */
        data = "{\n" +
                "  \t\"data\": [{\n" +
                "  \t\t\"id\": \"88888888888888888888888888\",\n" +
                "  \t\t\"price\": 9.1\n" +
                "         }],    \n" +
                "  \t\"type\": \"DELETE\",\n" +
                "  \t\"table\": \"order\",\n" +
                "  \t\"ts\": 1735375140701\n" +
                " }";

        record = formatFactory.parseRecord(structuredRecord, data.getBytes(TisUTF8.get()));
        Assert.assertNotNull(record);
        Assert.assertEquals(EventType.DELETE, record.getEventType());
        Assert.assertEquals(TEST_TABLE_NAME, record.tabName);
        afterVals = record.vals;
        Assert.assertNotNull(afterVals);
        Assert.assertEquals(2, afterVals.size());
        Assert.assertEquals("88888888888888888888888888", afterVals.get("id"));
        Assert.assertEquals(BigDecimal.valueOf(91, 1), (BigDecimal) afterVals.get("price"));

        beforeVals = record.getOldVals();
        Assert.assertNotNull(beforeVals);
        Assert.assertEquals(2, beforeVals.size());
        Assert.assertEquals("88888888888888888888888888", beforeVals.get("id"));
        Assert.assertEquals(BigDecimal.valueOf(91, 1), (BigDecimal) beforeVals.get("price"));


//        DataType row = DataTypes.ROW(
//                DataTypes.FIELD("id", DataTypes.STRING())
//                , DataTypes.FIELD("price", DataTypes.DECIMAL(5, 2)));
//
//        LookupRuntimeProviderContext context = new LookupRuntimeProviderContext(null);
//
//        DeserializationSchema<RowData> deserialize
//                = decodingFormat.createRuntimeDecoder(context, row);
//        Assert.assertEquals(CanalJsonDeserializationSchema.class, deserialize.getClass());
//
//        RowData rowData = deserialize.deserialize(data.getBytes(TisUTF8.get()));
//        Assert.assertNotNull("rowData can not be null", rowData);
    }

//    @Test
//    public void testCreateEncodingFormat() {
//        TISCanalJsonFormatFactory formatFactory = new TISCanalJsonFormatFactory();
//        EncodingFormat<SerializationSchema<RowData>> encodingFormat = formatFactory.createEncodingFormat(TEST_TABLE_NAME);
//
//        // encodingFormat.createRuntimeEncoder();
//
//    }
}