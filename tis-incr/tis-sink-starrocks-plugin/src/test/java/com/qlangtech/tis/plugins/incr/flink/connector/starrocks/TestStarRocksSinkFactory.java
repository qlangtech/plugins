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

package com.qlangtech.tis.plugins.incr.flink.connector.starrocks;

import com.google.common.collect.Maps;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.IDataxReader;
import com.qlangtech.tis.plugin.datax.SelectedTab;
import com.qlangtech.tis.plugin.datax.doris.DataXDorisWriter;
import com.qlangtech.tis.plugin.ds.ColumnMetaData;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.ds.doris.DorisSourceFactory;
import com.qlangtech.tis.realtime.transfer.DTO;
import com.qlangtech.tis.test.TISEasyMock;
import com.qlangtech.tis.trigger.util.JsonUtil;
import com.qlangtech.tis.util.DescriptorsJSON;
import com.starrocks.connector.flink.table.StarRocksSinkSemantic;
import junit.framework.TestCase;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.easymock.EasyMock;

import java.sql.Types;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-11-12 09:54
 **/
public class TestStarRocksSinkFactory extends TestCase implements TISEasyMock {

    public void testGetConfigOption() {
        String desc = StarRocksSinkFactory.desc("sinkSemantic");
        assertNotNull(desc);
    }

    public void testDescriptorsJSONGenerate() {
        StarRocksSinkFactory sinkFactory = new StarRocksSinkFactory();
        DescriptorsJSON descJson = new DescriptorsJSON(sinkFactory.getDescriptor());

        JsonUtil.assertJSONEqual(StarRocksSinkFactory.class, "starrocks-sink-factory.json"
                , descJson.getDescriptorsJSON(), (m, e, a) -> {
                    assertEquals(m, e, a);
                });

    }

    public void testStartRocksWrite() throws Exception {

        /**
         CREATE TABLE `totalpayinfo` (
         `id` varchar(32) NULL COMMENT "",
         `entity_id` varchar(10) NULL COMMENT "",
         `num` int(11) NULL COMMENT "",
         `create_time` bigint(20) NULL COMMENT "",
         `update_time` DATETIME   NULL,
         `update_date` DATE       NULL,
         `start_time`  DATETIME   NULL
         ) ENGINE=OLAP
         UNIQUE KEY(`id`)
         DISTRIBUTED BY HASH(`id`) BUCKETS 10
         PROPERTIES (
         "replication_num" = "1",
         "in_memory" = "false",
         "storage_format" = "DEFAULT"
         );
         * */

        String tableName = "totalpayinfo";
        String colEntityId = "entity_id";
        String colNum = "num";
        String colId = "id";
        String colCreateTime = "create_time";
        String updateTime = "update_time";
        String updateDate = "update_date";
        String starTime = "start_time";

        IDataxProcessor dataxProcessor = mock("dataxProcessor", IDataxProcessor.class);

        IDataxReader dataxReader = mock("dataxReader", IDataxReader.class);
        List<ISelectedTab> selectedTabs = Lists.newArrayList();
        SelectedTab totalpayinfo = mock(tableName, SelectedTab.class);
        EasyMock.expect(totalpayinfo.getName()).andReturn(tableName).times(1);
        List<ISelectedTab.ColMeta> cols = Lists.newArrayList();
        ISelectedTab.ColMeta cm = new ISelectedTab.ColMeta();
        cm.setName(colEntityId);
        cm.setType(new ColumnMetaData.DataType(Types.VARCHAR, 6));
        cols.add(cm);

        cm = new ISelectedTab.ColMeta();
        cm.setName(colNum);
        cm.setType(new ColumnMetaData.DataType(Types.INTEGER));
        cols.add(cm);

        cm = new ISelectedTab.ColMeta();
        cm.setName(colId);
        cm.setType(new ColumnMetaData.DataType(Types.VARCHAR, 32));
        cm.setPk(true);
        cols.add(cm);

        cm = new ISelectedTab.ColMeta();
        cm.setName(colCreateTime);
        cm.setType(new ColumnMetaData.DataType(Types.BIGINT));
        cols.add(cm);

        cm = new ISelectedTab.ColMeta();
        cm.setName(updateTime);
        cm.setType(new ColumnMetaData.DataType(Types.TIMESTAMP));
        cols.add(cm);

        cm = new ISelectedTab.ColMeta();
        cm.setName(updateDate);
        cm.setType(new ColumnMetaData.DataType(Types.DATE));
        cols.add(cm);

        cm = new ISelectedTab.ColMeta();
        cm.setName(starTime);
        cm.setType(new ColumnMetaData.DataType(Types.TIMESTAMP));
        cols.add(cm);

        EasyMock.expect(totalpayinfo.getCols()).andReturn(cols).times(2);
        selectedTabs.add(totalpayinfo);
        EasyMock.expect(dataxReader.getSelectedTabs()).andReturn(selectedTabs);

        EasyMock.expect(dataxProcessor.getReader(null)).andReturn(dataxReader);


        DataXDorisWriter dataXWriter = mock("dataXWriter", DataXDorisWriter.class);
        DorisSourceFactory sourceFactory = new DorisSourceFactory();
        sourceFactory.loadUrl = "[\"192.168.28.201:8030\"]";
        sourceFactory.userName = "root";
        sourceFactory.dbName = "tis";
        // sourceFactory.password = "";
        sourceFactory.port = 9030;
        sourceFactory.nodeDesc = "192.168.28.201";

        EasyMock.expect(dataXWriter.getDataSourceFactory()).andReturn(sourceFactory);

        dataXWriter.initWriterTable(tableName, Collections.singletonList("jdbc:mysql://192.168.28.201:9030/tis"));

        EasyMock.expect(dataxProcessor.getWriter(null)).andReturn(dataXWriter);

        StarRocksSinkFactory sinkFactory = new StarRocksSinkFactory();
        sinkFactory.columnSeparator = "x01";
        sinkFactory.rowDelimiter = "x02";
        sinkFactory.sinkSemantic = StarRocksSinkSemantic.AT_LEAST_ONCE.getName();
        sinkFactory.sinkBatchFlushInterval = 2000l;

        System.out.println("sinkFactory.sinkBatchFlushInterval:" + sinkFactory.sinkBatchFlushInterval);

        Map<String, IDataxProcessor.TableAlias> aliasMap = new HashMap<>();
        IDataxProcessor.TableAlias tab = new IDataxProcessor.TableAlias(tableName);
        aliasMap.put(tableName, tab);
        EasyMock.expect(dataxProcessor.getTabAlias()).andReturn(aliasMap);

        this.replay();
        Map<IDataxProcessor.TableAlias, SinkFunction<DTO>> sinkFunction = sinkFactory.createSinkFunction(dataxProcessor);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DTO d = new DTO();
        d.setEventType(DTO.EventType.DELETE);
        d.setTableName(tableName);
        Map<String, Object> after = Maps.newHashMap();
        after.put(colEntityId, "334556");
        after.put(colNum, "5");
        after.put(colId, "88888888887");
        after.put(colCreateTime, "20211113115959");
        after.put(updateTime, "2021-12-17T09:21:20Z");
        after.put(starTime, "2021-12-18 09:21:20");
        after.put(updateDate, "2021-12-9");
        d.setAfter(after);
        assertEquals(1, sinkFunction.size());
        for (Map.Entry<IDataxProcessor.TableAlias, SinkFunction<DTO>> entry : sinkFunction.entrySet()) {
            env.fromElements(new DTO[]{d}).addSink(entry.getValue());
            break;
        }

        env.execute("testJob");
        Thread.sleep(14000);

        this.verifyAll();
    }

}
