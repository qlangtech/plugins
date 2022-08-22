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

package com.qlangtech.plugins.incr.flink.chunjun.doris.sink;


import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.qlangtech.plugins.incr.flink.chunjun.sink.SinkTabPropsExtends;
import com.qlangtech.plugins.incr.flink.junit.TISApplySkipFlinkClassloaderFactoryCreation;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.IDataxReader;
import com.qlangtech.tis.datax.impl.DataxProcessor;
import com.qlangtech.tis.datax.impl.DataxWriter;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.manage.common.Config;
import com.qlangtech.tis.manage.common.TisUTF8;
import com.qlangtech.tis.plugin.KeyedPluginStore;
import com.qlangtech.tis.plugin.datax.SelectedTab;
import com.qlangtech.tis.plugin.datax.common.BasicDataXRdbmsWriter;
import com.qlangtech.tis.plugin.ds.BasicDataSourceFactory;
import com.qlangtech.tis.plugin.ds.DBConfig;
import com.qlangtech.tis.plugin.ds.DataType;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugins.incr.flink.connector.mysql.ChunjunSinkFactory;
import com.qlangtech.tis.plugins.incr.flink.connector.mysql.impl.ReplaceType;
import com.qlangtech.tis.realtime.DTOStream;
import com.qlangtech.tis.realtime.ReaderSource;
import com.qlangtech.tis.realtime.TabSinkFunc;
import com.qlangtech.tis.realtime.transfer.DTO;
import com.qlangtech.tis.test.TISEasyMock;
import org.apache.commons.io.FileUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.test.util.AbstractTestBase;
import org.easymock.EasyMock;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-08-17 16:07
 **/
public abstract class TestFlinkSinkExecutor extends AbstractTestBase implements TISEasyMock {


    protected static String dataXName = "testDataX";

    String tableName = "totalpayinfo";
    protected static final String dbName = "tis";

    String colEntityId = "entity_id";
    String colNum = "num";
    String colId = "id";
    String colCreateTime = "create_time";
    String updateTime = "update_time";
    String updateDate = "update_date";
    String starTime = "start_time";

    String pk = "88888888887";
    @ClassRule(order = 100)
    public static TestRule name = new TISApplySkipFlinkClassloaderFactoryCreation();

    static {
        System.setProperty(Config.SYSTEM_KEY_LOGBACK_PATH_KEY, "logback-test.xml");
    }

    protected abstract BasicDataSourceFactory getDsFactory();

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Ignore
    @Test
    public void test() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DTO d = createDTO(DTO.EventType.ADD);
        DTO update = createDTO(DTO.EventType.UPDATE_AFTER, (after) -> {
            after.put(colNum, 999);
        });

        // env.fromElements(new DTO[]{d, update}).addSink(new PrintSinkFunction<>());

        DTOStream sourceStream = DTOStream.createDispatched(tableName);
//
        ReaderSource<DTO> readerSource = ReaderSource.createDTOSource("testStreamSource", env.fromElements(new DTO[]{d, update}));
//
        readerSource.getSourceStream(env, Collections.singletonMap(tableName, sourceStream));
//
//
        //  dtoStream.addSink(new PrintSinkFunction<>());
        sourceStream.getStream().addSink(new PrintSinkFunction<>());

        // env.fromElements(new DTO[]{d}).addSink(entry.getValue());

        env.execute("testJob");

        Thread.sleep(5000);

    }

    // @Test
    protected void testSinkSync() throws Exception {


        //  System.out.println("logger.getClass():" + logger.getClass());

        /**
         建表需要将update_time作为uniqueKey的一部分，不然更新会有问题
         CREATE TABLE `totalpayinfo` (
         `id` varchar(32) NULL COMMENT "",
         `update_time` DATETIME   NULL,
         `entity_id` varchar(10) NULL COMMENT "",
         `num` int(11) NULL COMMENT "",
         `create_time` bigint(20) NULL COMMENT "",
         `update_date` DATE       NULL,
         `start_time`  DATETIME   NULL
         ) ENGINE=OLAP
         UNIQUE KEY(`id`,`update_time`)
         DISTRIBUTED BY HASH(`id`) BUCKETS 10
         PROPERTIES (
         "replication_num" = "1"
         );
         * */

        try {

            String[] colNames = new String[]{colEntityId, colNum, colId, colCreateTime, updateTime, updateDate, starTime};

            DataxProcessor dataxProcessor = mock("dataxProcessor", DataxProcessor.class);

            File ddlDir = folder.newFolder("ddl");
            String tabSql = tableName + IDataxProcessor.DATAX_CREATE_DDL_FILE_NAME_SUFFIX;
            FileUtils.write(new File(ddlDir, tabSql)
                    , IOUtils.loadResourceFromClasspath(this.getClass(), tabSql), TisUTF8.get());

            EasyMock.expect(dataxProcessor.getDataxCreateDDLDir(null)).andReturn(ddlDir);

            DataxProcessor.processorGetter = (name) -> {
                return dataxProcessor;
            };
            IDataxReader dataxReader = mock("dataxReader", IDataxReader.class);
            List<ISelectedTab> selectedTabs = Lists.newArrayList();


            SinkTabPropsExtends sinkExt = new SinkTabPropsExtends();
            sinkExt.tabName = tableName;


            //  EasyMock.expect(sinkExt.tabName).andReturn(tableName).times(2);
            //InsertType updateMode = new InsertType();

            ReplaceType updateMode = new ReplaceType();
            updateMode.updateKey = Lists.newArrayList(colId, updateTime);
            // EasyMock.expect(sinkExt.getIncrMode()).andReturn(updateMode);
            sinkExt.incrMode = updateMode;
            List<ISelectedTab.ColMeta> metaCols = Lists.newArrayList();
            ISelectedTab.ColMeta cm = new ISelectedTab.ColMeta();
            cm.setName(colEntityId);
            cm.setType(new DataType(Types.VARCHAR, "VARCHAR", 6));
            metaCols.add(cm);

            cm = new ISelectedTab.ColMeta();
            cm.setName(colNum);
            cm.setType(new DataType(Types.INTEGER));
            metaCols.add(cm);

            cm = new ISelectedTab.ColMeta();
            cm.setName(colId);
            cm.setType(new DataType(Types.VARCHAR, "VARCHAR", 32));
            cm.setPk(true);
            metaCols.add(cm);

            cm = new ISelectedTab.ColMeta();
            cm.setName(colCreateTime);
            cm.setType(new DataType(Types.BIGINT));
            metaCols.add(cm);

            cm = new ISelectedTab.ColMeta();
            cm.setName(updateTime);
            cm.setPk(true);
            cm.setType(new DataType(Types.TIMESTAMP));
            metaCols.add(cm);

            cm = new ISelectedTab.ColMeta();
            cm.setName(updateDate);
            cm.setType(new DataType(Types.DATE));
            metaCols.add(cm);

            cm = new ISelectedTab.ColMeta();
            cm.setName(starTime);
            cm.setType(new DataType(Types.TIMESTAMP));
            metaCols.add(cm);

            SelectedTab totalpayInfo = new SelectedTab() {
                @Override
                public List<ColMeta> getCols() {
                    return metaCols;
                }
            };
            totalpayInfo.setIncrSinkProps(sinkExt);
            totalpayInfo.name = tableName;


//            EasyMock.expect(sinkExt.getCols()).andReturn(metaCols).times(3);
            selectedTabs.add(totalpayInfo);
            EasyMock.expect(dataxReader.getSelectedTabs()).andReturn(selectedTabs).anyTimes();

            EasyMock.expect(dataxProcessor.getReader(null)).andReturn(dataxReader).anyTimes();

            // BasicDataSourceFactory dsFactory = MySqlContainer.createMySqlDataSourceFactory(new TargetResName(dataXName), MYSQL_CONTAINER);
            BasicDataXRdbmsWriter dataXWriter = createDataXWriter();

            dataXWriter.autoCreateTable = true;
            dataXWriter.dataXName = dataXName;
            // dataXWriter.maxBatchRows = 100;
            DataxWriter.dataxWriterGetter = (xName) -> {
                Assert.assertEquals(dataXName, xName);
                return dataXWriter;
            };

            // EasyMock.expect(dataXWriter.getDataSourceFactory()).andReturn(sourceFactory);

            //   dataXWriter.initWriterTable(tableName, Collections.singletonList("jdbc:mysql://192.168.28.201:9030/tis"));

            EasyMock.expect(dataxProcessor.getWriter(null)).andReturn(dataXWriter);

            ChunjunSinkFactory sinkFactory = getSinkFactory();
            sinkFactory.setKey(new KeyedPluginStore.Key(null, dataXName, null));
            sinkFactory.batchSize = 100;
            sinkFactory.flushIntervalMills = 100000;
            sinkFactory.semantic = "at-least-once";
            sinkFactory.parallelism = 1;


            Map<String, IDataxProcessor.TableAlias> aliasMap = new HashMap<>();
            IDataxProcessor.TableAlias tab = new IDataxProcessor.TableAlias(tableName);
            aliasMap.put(tableName, tab);
            EasyMock.expect(dataxProcessor.getTabAlias()).andReturn(aliasMap);

            this.replay();
            Map<IDataxProcessor.TableAlias, TabSinkFunc<RowData>> sinkFunction = sinkFactory.createSinkFunction(dataxProcessor);
            int updateNumVal = 999;
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            DTO add = createDTO(DTO.EventType.ADD);
            final DTO updateBefore = createDTO(DTO.EventType.UPDATE_BEFORE, (after) -> {
                after.put(colNum, updateNumVal);
                after.put(updateTime, "2021-12-17 09:21:22");
            });
            final DTO updateAfter = updateBefore.colone();
            updateAfter.setEventType(DTO.EventType.UPDATE_AFTER);

            final DTO delete = updateBefore.colone();
            delete.setEventType(DTO.EventType.DELETE);
//            d.setEventType(DTO.EventType.ADD);
//            d.setTableName(tableName);
//            Map<String, Object> after = Maps.newHashMap();
//            after.put(colEntityId, "334556");
//            after.put(colNum, 5);
//            String pk = "88888888887";
//            after.put(colId, pk);
//            after.put(colCreateTime, 20211113115959l);
//            //  after.put(updateTime, "2021-12-17T09:21:20Z");
//            after.put(updateTime, "2021-12-17 09:21:20");
//            after.put(starTime, "2021-12-18 09:21:20");
//            after.put(updateDate, "2021-12-09");
//            d.setAfter(after);
            Assert.assertEquals(1, sinkFunction.size());
            for (Map.Entry<IDataxProcessor.TableAlias, TabSinkFunc<RowData>> entry : sinkFunction.entrySet()) {

                DTOStream sourceStream = DTOStream.createDispatched(entry.getKey().getFrom());

                ReaderSource<DTO> readerSource = ReaderSource.createDTOSource("testStreamSource"
                        , env.fromElements(new DTO[]{add, updateBefore, updateAfter
                        }));

                readerSource.getSourceStream(env, Collections.singletonMap(tableName, sourceStream));

                entry.getValue().add2Sink(sourceStream);

                //  sourceStream.getStream().addSink();

                // entry.getValue().add2Sink(sourceStream.addStream(env.fromElements(new DTO[]{d, update})));
                // env.fromElements(new DTO[]{d}).addSink(entry.getValue());
                break;
            }

            env.execute("testJob");
            Thread.sleep(9000);


            DBConfig dbConfig = this.getDsFactory().getDbConfig();

            String[] jdbcUrls = new String[1];
            dbConfig.vistDbURL(false, (dbName, dbHost, jdbcUrl) -> {
                jdbcUrls[0] = jdbcUrl;
            });

            try (Connection conn = this.getDsFactory().getConnection(jdbcUrls[0])) {
                try (Statement statement = conn.createStatement()) {
                    try (ResultSet resultSet = statement.executeQuery("select * from " + tableName + " where id=" + pk)) {
                        if (resultSet.next()) {
                            StringBuffer rowDesc = new StringBuffer();
                            for (String col : colNames) {
                                Object obj = resultSet.getObject(col);
                                rowDesc.append(col).append("=").append(obj).append("[").append((obj != null) ? obj.getClass().getSimpleName() : "").append("]").append(" , ");
                            }
                            Assert.assertEquals(updateNumVal, resultSet.getInt(colNum));
                            System.out.println("test_output==>" + rowDesc.toString());
                        } else {
                            Assert.fail("have not find row with id=" + pk);
                        }
                    }
                }
            }

//            DBConfig dbConfig = dsFactory.getDbConfig();
//            dbConfig.vistDbURL(false, (dbName, dbHost, jdbcUrl) -> {
//                try (Connection conn = dsFactory.getConnection(jdbcUrl)) {
//
//
//
//                } catch (Exception e) {
//                    throw new RuntimeException(e);
//                }
//            });
            this.verifyAll();


        } catch (Throwable e) {
            Thread.sleep(14000);
            throw new RuntimeException(e);
        }
    }

    protected abstract ChunjunSinkFactory getSinkFactory();

    protected abstract BasicDataXRdbmsWriter createDataXWriter();

    protected DTO createDTO(DTO.EventType eventType, Consumer<Map<String, Object>>... consumer) {
        DTO d = new DTO();
        d.setEventType(eventType);
        d.setTableName(tableName);
        Map<String, Object> after = Maps.newHashMap();
        after.put(colEntityId, "334556");
        after.put(colNum, 5);
        after.put(colId, pk);
        after.put(colCreateTime, 20211113115959l);
        //  after.put(updateTime, "2021-12-17T09:21:20Z");
        after.put(updateTime, "2021-12-17 09:21:20");
        after.put(starTime, "2021-12-18 09:21:20");
        after.put(updateDate, "2021-12-09");
        d.setAfter(after);
        if (eventType != DTO.EventType.ADD) {
            d.setBefore(Maps.newHashMap(after));
            for (Consumer<Map<String, Object>> c : consumer) {
                c.accept(after);
            }
        }
        return d;
    }
}
