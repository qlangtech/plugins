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
import com.qlangtech.tis.datax.impl.DataxWriter;
import com.qlangtech.tis.plugin.datax.BasicDorisStarRocksWriter;
import com.qlangtech.tis.plugin.datax.SelectedTab;
import com.qlangtech.tis.plugin.datax.doris.DataXDorisWriter;
import com.qlangtech.tis.plugin.ds.DataType;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.ds.doris.DorisSourceFactory;
import com.qlangtech.tis.realtime.DTOStream;
import com.qlangtech.tis.realtime.TabSinkFunc;
import com.qlangtech.tis.realtime.transfer.DTO;
import com.qlangtech.tis.test.TISEasyMock;
import com.qlangtech.tis.trigger.util.JsonUtil;
import com.qlangtech.tis.util.DescriptorsJSON;
import com.starrocks.connector.flink.table.sink.StarRocksSinkSemantic;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.File;
import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-11-12 09:54
 **/
public class TestStarRocksSinkFactory implements TISEasyMock {

    private static final int DORIS_FE_PORT = 9030;
    private static final int DORIS_FE_LOAD_PORT = 8030;
    private static final int DORIS_BE_PORT = 9050;
    private static final int DORIS_BE_LOAD_PORT = 8040;
    private static final String DORIS_FE_SERVICE = "doris-fe_1";
    private static final String DORIS_BE_SERVICE = "doris-be_1";

    @ClassRule
    public static DockerComposeContainer environment =
            new DockerComposeContainer(new File("src/test/resources/compose-starrocks-test.yml"))
                    .withExposedService(DORIS_FE_SERVICE, DORIS_FE_PORT)
                    .withExposedService(DORIS_FE_SERVICE, DORIS_FE_LOAD_PORT)
                    .withExposedService(DORIS_BE_SERVICE, DORIS_BE_PORT)
                    .withExposedService(DORIS_BE_SERVICE, DORIS_BE_LOAD_PORT);

    // docker run -d -p 1521:1521 -e ORACLE_PASSWORD=test -e ORACLE_DATABASE=tis gvenzl/oracle-xe:18.4.0-slim
    public static final DockerImageName STARROCKS_DOCKER_IMAGE_NAME = DockerImageName.parse(
            "tis/starrocks"
            // "registry.cn-hangzhou.aliyuncs.com/tis/oracle-xe:18.4.0-slim"
    );

    @BeforeClass
    public static void initialize() {
        GenericContainer starRocksContainer = new GenericContainer(STARROCKS_DOCKER_IMAGE_NAME);
        starRocksContainer.start();
    }


    public void testGetConfigOption() {
        String desc = StarRocksSinkFactory.desc("sinkSemantic");
        Assert.assertNotNull(desc);
    }

    public void testDescriptorsJSONGenerate() {
        StarRocksSinkFactory sinkFactory = new StarRocksSinkFactory();
        DescriptorsJSON descJson = new DescriptorsJSON(sinkFactory.getDescriptor());

        JsonUtil.assertJSONEqual(StarRocksSinkFactory.class, "starrocks-sink-factory.json"
                , descJson.getDescriptorsJSON(), (m, e, a) -> {
                    Assert.assertEquals(m, e, a);
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

        String dataXName = "testDataX";

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
        cm.setType(new DataType(Types.VARCHAR, "VARCHAR", 6));
        cols.add(cm);

        cm = new ISelectedTab.ColMeta();
        cm.setName(colNum);
        cm.setType(new DataType(Types.INTEGER));
        cols.add(cm);

        cm = new ISelectedTab.ColMeta();
        cm.setName(colId);
        cm.setType(new DataType(Types.VARCHAR, "VARCHAR", 32));
        cm.setPk(true);
        cols.add(cm);

        cm = new ISelectedTab.ColMeta();
        cm.setName(colCreateTime);
        cm.setType(new DataType(Types.BIGINT));
        cols.add(cm);

        cm = new ISelectedTab.ColMeta();
        cm.setName(updateTime);
        cm.setType(new DataType(Types.TIMESTAMP));
        cols.add(cm);

        cm = new ISelectedTab.ColMeta();
        cm.setName(updateDate);
        cm.setType(new DataType(Types.DATE));
        cols.add(cm);

        cm = new ISelectedTab.ColMeta();
        cm.setName(starTime);
        cm.setType(new DataType(Types.TIMESTAMP));
        cols.add(cm);

        EasyMock.expect(totalpayinfo.getCols()).andReturn(cols).times(2);


        selectedTabs.add(totalpayinfo);
        EasyMock.expect(dataxReader.getSelectedTabs()).andReturn(selectedTabs);

        EasyMock.expect(dataxProcessor.getReader(null)).andReturn(dataxReader);

        DorisSourceFactory sourceFactory = new DorisSourceFactory();
        sourceFactory.loadUrl = "[\"192.168.28.201:8030\"]";
        sourceFactory.userName = "root";
        sourceFactory.dbName = "tis";
        // sourceFactory.password = "";
        sourceFactory.port = 9030;
        sourceFactory.nodeDesc = "192.168.28.201";

        DataXDorisWriter dataXWriter = new DataXDorisWriter() {
            @Override
            public Separator getSeparator() {
                return new BasicDorisStarRocksWriter.Separator() {
                    @Override
                    public String getColumnSeparator() {
                        return COL_SEPARATOR_DEFAULT;
                    }

                    @Override
                    public String getRowDelimiter() {
                        return ROW_DELIMITER_DEFAULT;
                    }
                };
            }

            @Override
            public DorisSourceFactory getDataSourceFactory() {
                return sourceFactory;
            }
        }; //mock("dataXWriter", DataXDorisWriter.class);

        dataXWriter.dataXName = dataXName;
        DataxWriter.dataxWriterGetter = (xName) -> {
            Assert.assertEquals(dataXName, xName);
            return dataXWriter;
        };

        // EasyMock.expect(dataXWriter.getDataSourceFactory()).andReturn(sourceFactory);

        //   dataXWriter.initWriterTable(tableName, Collections.singletonList("jdbc:mysql://192.168.28.201:9030/tis"));

        EasyMock.expect(dataxProcessor.getWriter(null)).andReturn(dataXWriter);

        StarRocksSinkFactory sinkFactory = new StarRocksSinkFactory();
//        sinkFactory.columnSeparator = "x01";
//        sinkFactory.rowDelimiter = "x02";
        sinkFactory.sinkSemantic = StarRocksSinkSemantic.AT_LEAST_ONCE.getName();
        sinkFactory.sinkBatchFlushInterval = 2000l;

        System.out.println("sinkFactory.sinkBatchFlushInterval:" + sinkFactory.sinkBatchFlushInterval);

        Map<String, IDataxProcessor.TableAlias> aliasMap = new HashMap<>();
        IDataxProcessor.TableAlias tab = new IDataxProcessor.TableAlias(tableName);
        aliasMap.put(tableName, tab);
        EasyMock.expect(dataxProcessor.getTabAlias()).andReturn(aliasMap);

        this.replay();
        Map<IDataxProcessor.TableAlias, TabSinkFunc<DTO>> sinkFunction = sinkFactory.createSinkFunction(dataxProcessor);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DTO d = new DTO();
        d.setEventType(DTO.EventType.ADD);
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
        Assert.assertEquals(1, sinkFunction.size());
        for (Map.Entry<IDataxProcessor.TableAlias, TabSinkFunc<DTO>> entry : sinkFunction.entrySet()) {

            entry.getValue().add2Sink(DTOStream.createDispatched(tableName).addStream(env.fromElements(new DTO[]{d})));

            // env.fromElements(new DTO[]{d}).addSink(entry.getValue());
            break;
        }

        env.execute("testJob");
        Thread.sleep(14000);

        this.verifyAll();
    }

}
