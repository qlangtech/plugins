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

package com.qlangtech.tis.plugins.incr.flink.connector.clickhouse;

import com.google.common.collect.Maps;
import com.qlangtech.plugins.incr.flink.junit.TISApplySkipFlinkClassloaderFactoryCreation;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.IDataxReader;
import com.qlangtech.tis.datax.impl.DataxProcessor;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.manage.common.TisUTF8;
import com.qlangtech.tis.plugin.datax.DataXClickhouseWriter;
import com.qlangtech.tis.plugin.datax.SelectedTab;
import com.qlangtech.tis.plugin.ds.DataType;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.ds.clickhouse.ClickHouseDataSourceFactory;
import com.qlangtech.tis.realtime.TabSinkFunc;
import com.qlangtech.tis.realtime.transfer.DTO;
import com.qlangtech.tis.test.TISEasyMock;
import com.qlangtech.tis.trigger.util.JsonUtil;
import com.qlangtech.tis.util.DescriptorsJSON;
import org.apache.commons.compress.utils.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;

import java.io.File;
import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-12-02 09:13
 **/
public class TestClickHouseSinkFactory implements TISEasyMock {

    //    public void test() {
//        Path path = Paths.get("/tmp/tis-clickhouse-sink");
//        System.out.println(Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS));
//    }
    @ClassRule(order = 100)
    public static TestRule name = new TISApplySkipFlinkClassloaderFactoryCreation();

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testDescriptorsJSONGenerate() {
        ClickHouseSinkFactory sinkFactory = new ClickHouseSinkFactory();
        DescriptorsJSON descJson = new DescriptorsJSON(sinkFactory.getDescriptor());

        JsonUtil.assertJSONEqual(ClickHouseSinkFactory.class, "clickhouse-sink-factory.json"
                , descJson.getDescriptorsJSON(), (m, e, a) -> {
                    Assert.assertEquals(m, e, a);
                });

    }

    @Test
    public void testCreateSinkFunction() throws Exception {
        String testDataX = "testDataX";
        String tableName = "totalpayinfo";
        // cols
        String colEntityId = "entity_id";
        String colNum = "num";
        String colId = "id";
        String colCreateTime = "create_time";

        DataxProcessor dataxProcessor = mock("dataxProcessor", DataxProcessor.class);

        File ddlDir = folder.newFolder("ddl");

        FileUtils.write(new File(ddlDir, tableName + ".sql")
                , IOUtils.loadResourceFromClasspath(TestClickHouseSinkFactory.class, "totalpay-create-ddl.sql"), TisUTF8.get());

        EasyMock.expect(dataxProcessor.getDataxCreateDDLDir(null)).andReturn(ddlDir);

        IDataxReader dataxReader = mock("dataxReader", IDataxReader.class);
        List<ISelectedTab> selectedTabs = Lists.newArrayList();
        SelectedTab totalpayinfo = mock(tableName, SelectedTab.class);
        EasyMock.expect(totalpayinfo.getName()).andReturn(tableName);
        List<ISelectedTab.ColMeta> cols = Lists.newArrayList();
        ISelectedTab.ColMeta cm = new ISelectedTab.ColMeta();
        cm.setName(colEntityId);
        cm.setType(new DataType(Types.VARCHAR, 6));
        cols.add(cm);

        cm = new ISelectedTab.ColMeta();
        cm.setName(colNum);
        cm.setType(new DataType(Types.INTEGER));
        cols.add(cm);

        cm = new ISelectedTab.ColMeta();
        cm.setName(colId);
        cm.setType(new DataType(Types.VARCHAR, 32));
        cm.setPk(true);
        cols.add(cm);

        cm = new ISelectedTab.ColMeta();
        cm.setName(colCreateTime);
        cm.setType(new DataType(Types.BIGINT));
        cols.add(cm);

        EasyMock.expect(totalpayinfo.getCols()).andReturn(cols).anyTimes();
        selectedTabs.add(totalpayinfo);
        EasyMock.expect(dataxReader.getSelectedTabs()).andReturn(selectedTabs);

        EasyMock.expect(dataxProcessor.getReader(null)).andReturn(dataxReader);


        //mock("dataXWriter", DataXClickhouseWriter.class);
        //  dataXWriter.initWriterTable(tableName, Collections.singletonList("jdbc:clickhouse://192.168.28.201:8123/tis"));


        final ClickHouseDataSourceFactory sourceFactory = new ClickHouseDataSourceFactory();

        sourceFactory.userName = "default";
        sourceFactory.dbName = "tis";
        sourceFactory.password = "123456";
        sourceFactory.port = 8123;
        sourceFactory.nodeDesc = "192.168.28.201";

        DataXClickhouseWriter dataXWriter = new DataXClickhouseWriter() {
            @Override
            public ClickHouseDataSourceFactory getDataSourceFactory() {
                // return super.getDataSourceFactory();
                return sourceFactory;
            }
        };
        dataXWriter.dataXName = testDataX;
        dataXWriter.autoCreateTable = true;

        //EasyMock.expect(dataXWriter.getDataSourceFactory()).andReturn(sourceFactory);

        EasyMock.expect(dataxProcessor.getWriter(null)).andReturn(dataXWriter);
        DataxProcessor.processorGetter = (name) -> {
            return dataxProcessor;
        };

        Map<String, IDataxProcessor.TableAlias> aliasMap = new HashMap<>();
        IDataxProcessor.TableAlias tab = new IDataxProcessor.TableAlias(tableName);
        aliasMap.put(tableName, tab);
        EasyMock.expect(dataxProcessor.getTabAlias()).andReturn(aliasMap);

        this.replay();

        ClickHouseSinkFactory clickHouseSinkFactory = new ClickHouseSinkFactory();
        clickHouseSinkFactory.ignoringSendingException = true;
        clickHouseSinkFactory.maxBufferSize = 1;
        clickHouseSinkFactory.numRetries = 5;
        clickHouseSinkFactory.numWriters = 1;
        clickHouseSinkFactory.queueMaxCapacity = 1;
        clickHouseSinkFactory.timeout = 30;

        Map<IDataxProcessor.TableAlias, TabSinkFunc<DTO>>
                sinkFuncs = clickHouseSinkFactory.createSinkFunction(dataxProcessor);
        Assert.assertTrue(sinkFuncs.size() > 0);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DTO d = new DTO();
        d.setTableName(tableName);
        Map<String, Object> after = Maps.newHashMap();
        after.put(colEntityId, "334556");
        after.put(colNum, "5");
        after.put(colId, "123dsf124325253dsf123");
        after.put(colCreateTime, "20211113115959");
        d.setAfter(after);
        Assert.assertEquals(1, sinkFuncs.size());

        for (Map.Entry<IDataxProcessor.TableAlias, TabSinkFunc<DTO>> entry : sinkFuncs.entrySet()) {
            // .addSink(entry.getValue().).name("clickhouse");
            entry.getValue().add2Sink(env.fromElements(new DTO[]{d}));
            break;
        }

        env.execute("testJob");

        Thread.sleep(5000);

        this.verifyAll();
    }
}
