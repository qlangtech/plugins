/**
 * Copyright (c) 2020 QingLang, Inc. <baisui@qlangtech.com>
 * <p>
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 * <p>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.qlangtech.tis.plugins.incr.flink.connector.starrocks;

import com.google.common.collect.Maps;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.IDataxReader;
import com.qlangtech.tis.plugin.datax.doris.DataXDorisWriter;
import com.qlangtech.tis.plugin.datax.SelectedTab;
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
         `create_time` bigint(20) NULL COMMENT ""
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

        IDataxProcessor dataxProcessor = mock("dataxProcessor", IDataxProcessor.class);

        IDataxReader dataxReader = mock("dataxReader", IDataxReader.class);
        List<ISelectedTab> selectedTabs = Lists.newArrayList();
        SelectedTab totalpayinfo = mock(tableName, SelectedTab.class);
        EasyMock.expect(totalpayinfo.getName()).andReturn(tableName).times(2);
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

        EasyMock.expect(totalpayinfo.getCols()).andReturn(cols).times(2);
        selectedTabs.add(totalpayinfo);
        EasyMock.expect(dataxReader.getSelectedTabs()).andReturn(selectedTabs);

        EasyMock.expect(dataxProcessor.getReader(null)).andReturn(dataxReader);


        DataXDorisWriter dataXWriter = mock("dataXWriter", DataXDorisWriter.class);
        DorisSourceFactory sourceFactory = new DorisSourceFactory();
        sourceFactory.loadUrl = "192.168.28.201:8030";
        sourceFactory.userName = "root";
        sourceFactory.dbName = "tis";
        // sourceFactory.password = "";
        sourceFactory.port = 9030;
        sourceFactory.nodeDesc = "192.168.28.201";

        EasyMock.expect(dataXWriter.getDataSourceFactory()).andReturn(sourceFactory);

        EasyMock.expect(dataxProcessor.getWriter(null)).andReturn(dataXWriter);

        StarRocksSinkFactory sinkFactory = new StarRocksSinkFactory();
        sinkFactory.columnSeparator = "\\x01";
        sinkFactory.rowDelimiter = "\\x02";
        sinkFactory.sinkSemantic = StarRocksSinkSemantic.AT_LEAST_ONCE.getName();

        Map<String, IDataxProcessor.TableAlias> aliasMap = new HashMap<>();
        IDataxProcessor.TableAlias tab = new IDataxProcessor.TableAlias(tableName);
        aliasMap.put(tableName, tab);
        EasyMock.expect(dataxProcessor.getTabAlias()).andReturn(aliasMap);

        this.replay();
        SinkFunction<DTO> sinkFunction = sinkFactory.createSinkFunction(dataxProcessor);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DTO d = new DTO();
        d.setTableName(tableName);
        Map<String, Object> after = Maps.newHashMap();
        after.put(colEntityId, "334556");
        after.put(colNum, "5");
        after.put(colId, "123dsf124325253dsf123");
        after.put(colCreateTime, "20211113115959");
        d.setAfter(after);
        env.fromElements(new DTO[]{d}).addSink(sinkFunction);

        env.execute("testJob");

        this.verifyAll();
    }

}
