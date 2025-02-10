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
import com.qlangtech.plugins.incr.flink.cdc.FlinkCol;
import com.qlangtech.plugins.incr.flink.cdc.IResultRows;
import com.qlangtech.plugins.incr.flink.junit.TISApplySkipFlinkClassloaderFactoryCreation;
import com.qlangtech.tis.async.message.client.consumer.IFlinkColCreator;
import com.qlangtech.tis.async.message.client.consumer.IMQListener;
import com.qlangtech.tis.async.message.client.consumer.Tab2OutputTag;
import com.qlangtech.tis.async.message.client.consumer.impl.MQListenerFactory;
import com.qlangtech.tis.datax.DataXCfgFile;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.IDataxReader;
import com.qlangtech.tis.datax.SourceColMetaGetter;
import com.qlangtech.tis.datax.TableAlias;
import com.qlangtech.tis.datax.TableAliasMapper;
import com.qlangtech.tis.datax.impl.DataxProcessor;
import com.qlangtech.tis.datax.impl.DataxReader;
import com.qlangtech.tis.datax.impl.DataxWriter;
import com.qlangtech.tis.manage.common.Config;
import com.qlangtech.tis.manage.common.TisUTF8;
import com.qlangtech.tis.plugin.KeyedPluginStore;
import com.qlangtech.tis.plugin.datax.AbstractCreateTableSqlBuilder.CreateDDL;
import com.qlangtech.tis.plugin.datax.CreateTableSqlBuilder;
import com.qlangtech.tis.plugin.datax.SelectedTab;
import com.qlangtech.tis.plugin.datax.common.AutoCreateTable;
import com.qlangtech.tis.plugin.datax.common.BasicDataXRdbmsWriter;
import com.qlangtech.tis.plugin.ds.BasicDataSourceFactory;
import com.qlangtech.tis.plugin.ds.CMeta;
import com.qlangtech.tis.plugin.ds.DataType;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.ds.JDBCTypes;
import com.qlangtech.tis.plugin.incr.TISSinkFactory;
import com.qlangtech.tis.plugins.incr.flink.cdc.AbstractRowDataMapper;
import com.qlangtech.tis.plugins.incr.flink.chunjun.sink.SinkTabPropsExtends;
import com.qlangtech.tis.plugins.incr.flink.connector.ChunjunSinkFactory;
import com.qlangtech.tis.plugins.incr.flink.connector.UpdateMode;
import com.qlangtech.tis.plugins.incr.flink.connector.impl.ReplaceType;
import com.qlangtech.tis.realtime.ReaderSource;
import com.qlangtech.tis.realtime.TabSinkFunc;
import com.qlangtech.tis.realtime.dto.DTOStream;
import com.qlangtech.tis.realtime.transfer.DTO;
import com.qlangtech.tis.test.TISEasyMock;
import com.qlangtech.tis.util.HeteroEnum;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.test.util.AbstractTestBase;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;

import java.io.File;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-08-17 16:07
 **/
public abstract class TestFlinkSinkExecutor extends AbstractTestBase implements TISEasyMock {


    protected static String dataXName = "testDataX";

    protected static final String tableName = "totalpayinfo";
    protected static final String dbName = "tis";

    String colEntityId = "entity_id";
    protected final String colNum = "num";
    static protected String colId = "id";
    String colCreateTime = "create_time";
    protected String updateTime = "update_time";
    String updateDate = "update_date";
    static String starTime = "start_time";
    static String price = "price";

    String pk = "88888888887";
    public static final String newAddedRecord = "88888888889";
    public static final String deleteFinallyId = "88888888890";


    final String colEntityIdVal = "334556";
    final int colNumVal = 5;
    final int colNumValUpdated = 999;
    final long colCreateTimeVal = 20211113115959l;
    final BigDecimal priceVal = new BigDecimal("1314.99");

    final String updateDateVal = "2021-12-09";
    final String starTimeVal = "2021-12-18 09:21:20";
    final String updateTimeVal = "2021-12-17 09:21:20";
    final String updateTimeValUpdated = "2021-12-17 09:21:22";

    @ClassRule(order = 100)
    public static TestRule name = new TISApplySkipFlinkClassloaderFactoryCreation();

    static {
        System.setProperty(Config.SYSTEM_KEY_LOGBACK_PATH_KEY, "logback-test.xml");
    }

    protected abstract BasicDataSourceFactory getDsFactory();

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Before
    public void startClearMocks() {
        this.clearMocks();
    }

    @Ignore
    @Test
    public void test() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DTO d = createDTO(DTO.EventType.ADD);
        DTO update = createDTO(DTO.EventType.UPDATE_AFTER, (after) -> {
            after.put(colNum, colNumValUpdated);
        });

        // env.fromElements(new DTO[]{d, update}).addSink(new PrintSinkFunction<>());

        DTOStream sourceStream = DTOStream.createDispatched(tableName);
        //
        ReaderSource<DTO> readerSource = ReaderSource.createDTOSource("testStreamSource",
                env.fromElements(new DTO[]{d, update}));
        //
        readerSource.getSourceStream(env, new Tab2OutputTag<>(Collections.singletonMap(new TableAlias(tableName),
                sourceStream)));
        //
        //
        //  dtoStream.addSink(new PrintSinkFunction<>());
        sourceStream.getStream().addSink(new PrintSinkFunction<>());

        // env.fromElements(new DTO[]{d}).addSink(entry.getValue());

        env.execute("testJob");

        Thread.sleep(3000);

    }

    //  protected int updateNumVal = 999;

//    protected DTO[] createTestDTO() {
//        return createTestDTO(true);
//    }


    public interface FlinkTestCase {
        /**
         * 添加测试记录
         *
         * @return
         */
        public List<DTO> createTestData();

        /**
         * 生成对应的校验逻辑
         *
         * @param ddl
         * @param statement
         * @throws SQLException
         */
        public void verifyRelevantRow(CreateDDL ddl, Statement statement) throws SQLException;
    }


    /**
     * 创建一条针对同一
     *
     * @param needDelete
     * @return
     */
    private List<DTO> createDTOsForOneSingleRecord(boolean needDelete) {
        DTO add = createDTO(DTO.EventType.ADD);
        final DTO updateBefore = createDTO(DTO.EventType.UPDATE_BEFORE, (after) -> {
            after.put(colNum, colNumValUpdated);
            after.put(updateTime, updateTimeValUpdated);
        });
        final DTO updateAfter = updateBefore.clone();
        updateAfter.setEventType(DTO.EventType.UPDATE_AFTER);
        List<DTO> dtos = Lists.newArrayList(add, updateAfter);

        if (needDelete) {
            final DTO delete = updateBefore.clone();
            delete.setBefore(delete.getAfter());
            delete.setEventType(DTO.EventType.DELETE);
            dtos.add(delete);
        }
        return dtos;
    }

    /**
     * 尝试使用Stream API流程测试
     *
     * @throws Exception
     */
    protected void testSinkSync() throws Exception {

        final IStreamScriptRun streamScriptRun = new IStreamScriptRun() {
            @Override
            public void runStream(DataxProcessor dataxProcessor, ChunjunSinkFactory sinkFactory, StreamExecutionEnvironment env, SelectedTab selectedTab) throws Exception {
                IFlinkColCreator flinkColCreator = null;
                Map<TableAlias, TabSinkFunc<RowData>> sinkFunction = sinkFactory.createSinkFunction(dataxProcessor, flinkColCreator);
                Assert.assertEquals(1, sinkFunction.size());
                TestFlinkSinkExecutor.this.startTestSinkSync(sinkFunction);


                for (Map.Entry<TableAlias, TabSinkFunc<RowData>> entry : sinkFunction.entrySet()) {

                    Pair<DTOStream, ReaderSource<DTO>> sourceStream = createReaderSource(env, entry.getKey(), this);

                    entry.getValue().add2Sink(sourceStream.getKey());

                    //  sourceStream.getStream().addSink();

                    // entry.getValue().add2Sink(sourceStream.addStream(env.fromElements(new DTO[]{d, update})));
                    // env.fromElements(new DTO[]{d}).addSink(entry.getValue());
                    break;
                }

                env.execute("testJob");
            }


        };


        this.testSinkSync(streamScriptRun);
    }

//    protected DTO[] createTestDTO() {
//
//        // 创建一条更新记录
//        List<DTO> dtos = createDTOsForOneSingleRecord(false);
//
//        // 创建一条添加记录
//        DTO newAdd = createDTO(DTO.EventType.ADD, (after) -> {
//            after.put(colId, newAddedRecord);
//        });
//
//        dtos.add(newAdd);
//
//
//        // 创建三条event，最终该记录会被删除
//        List<DTO> deleteFinally = this.createDTOsForOneSingleRecord(true);
//        Map<String, Object> after;
//        Map<String, Object> before;
//        for (DTO d : deleteFinally) {
//            after = d.getAfter();
//            before = d.getBefore();
//            if (MapUtils.isNotEmpty(after)) {
//                after.put(colId, deleteFinallyId);
//            }
//            if (MapUtils.isNotEmpty(before)) {
//                before.put(colId, deleteFinallyId);
//            }
//            // 三条： 1.添加 2.更新 3.删除
//            dtos.add(d);
//        }
//
//        Assert.assertEquals(7, dtos.size());
//        return dtos.toArray(new DTO[dtos.size()]); //new DTO[]{add, updateAfter};
//    }

    protected void startTestSinkSync(Map<TableAlias, TabSinkFunc<RowData>> sinkFunction) {
    }

    // @Test
    protected void testSinkSync(IStreamScriptRun streamScriptRun) throws Exception {

        List<FlinkTestCase> flinkTestCases = streamScriptRun.createFlinkTestCases();

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
         `price` decimal(10 , 2) null,
         `start_time`  DATETIME   NULL
         ) ENGINE=OLAP
         UNIQUE KEY(`id`,`update_time`)
         DISTRIBUTED BY HASH(`id`) BUCKETS 10
         PROPERTIES (
         "replication_num" = "1"
         );
         * */

        try {

            // 定义MetaData
            SelectedTab totalpayInfo = createSelectedTab();
            // tableName = totalpayInfo.getName();
            DataxProcessor dataxProcessor = mock("dataxProcessor", DataxProcessor.class);
            Map<String, TableAlias> mapper = Maps.newHashMap();
            mapper.put(tableName, new TableAlias(tableName));
            TableAliasMapper aliasMapper = new TableAliasMapper(mapper);
            EasyMock.expect(dataxProcessor.getTabAlias(null)).andReturn(aliasMapper);
            EasyMock.expect(dataxProcessor.identityValue()).andReturn(dataXName).anyTimes();

            File ddlDir = folder.newFolder("ddl");
            String tabSql = tableName + DataXCfgFile.DATAX_CREATE_DDL_FILE_NAME_SUFFIX;


            EasyMock.expect(dataxProcessor.getDataxCreateDDLDir(null)).andReturn(ddlDir).anyTimes();

            DataxProcessor.processorGetter = (name) -> {
                return dataxProcessor;
            };
            HeteroEnum.incrSourceListenerFactoryStub = (dataX) -> {

                MQListenerFactory mockIncrSourceFactory = new MQListenerFactory() {
                    @Override
                    public IFlinkColCreator<FlinkCol> createFlinkColCreator() {
                        return AbstractRowDataMapper::mapFlinkCol;
                    }

                    @Override
                    public IMQListener create() {
                        throw new UnsupportedOperationException();
                    }
                };
                return mockIncrSourceFactory;
            };
            IDataxReader dataxReader = createDataxReader();
            List<ISelectedTab> selectedTabs = Lists.newArrayList();


            //            EasyMock.expect(sinkExt.getCols()).andReturn(metaCols).times(3);
            selectedTabs.add(totalpayInfo);
            EasyMock.expect(dataxReader.getSelectedTabs()).andReturn(selectedTabs).anyTimes();

            EasyMock.expect(dataxProcessor.getReader(null)).andReturn(dataxReader).anyTimes();

            // BasicDataSourceFactory dsFactory = MySqlContainer.createMySqlDataSourceFactory(new TargetResName
            // (dataXName), MYSQL_CONTAINER);
            DataxWriter dataXWriter = createDataXWriter();

            if (dataXWriter instanceof BasicDataXRdbmsWriter) {
                BasicDataXRdbmsWriter rdbmsWriter = (BasicDataXRdbmsWriter) dataXWriter;
                rdbmsWriter.autoCreateTable = AutoCreateTable.dft();
                rdbmsWriter.dataXName = dataXName;
            }


            // dataXWriter.maxBatchRows = 100;
            DataxWriter.dataxWriterGetter = (xName) -> {
                Assert.assertEquals(dataXName, xName);
                return dataXWriter;
            };

            // Assert.assertTrue("autoCreateTable must be true", dataXWriter.autoCreateTable);
            CreateTableSqlBuilder.CreateDDL createDDL = null;
            if (!dataXWriter.isGenerateCreateDDLSwitchOff()) {
                createDDL = dataXWriter.generateCreateDDL(SourceColMetaGetter.getNone(), new IDataxProcessor.TableMap(totalpayInfo), Optional.empty());
                Assert.assertNotNull("createDDL can not be empty", createDDL);
                // log.info("create table ddl:\n{}", createDDL);
                FileUtils.write(new File(ddlDir, tabSql), createDDL.getDDLScript(), TisUTF8.get());
            }
            // EasyMock.expect(dataXWriter.getDataSourceFactory()).andReturn(sourceFactory);

            //   dataXWriter.initWriterTable(tableName, Collections.singletonList("jdbc:mysql://192.168.28
            //   .201:9030/tis"));

            EasyMock.expect(dataxProcessor.getWriter(null)).andReturn(dataXWriter).anyTimes();

            ChunjunSinkFactory sinkFactory = getSinkFactory();
            sinkFactory.setKey(new KeyedPluginStore.Key(null, dataXName, null));
            sinkFactory.batchSize = 100;
            sinkFactory.flushIntervalMills = 100000;
            sinkFactory.semantic = "at-least-once";
            sinkFactory.parallelism = 1;
            TISSinkFactory.stubGetter = (pn) -> {
                return sinkFactory;
            };

            Map<String, TableAlias> aliasMap = new HashMap<>();
            TableAlias tab = new TableAlias(tableName);
            aliasMap.put(tableName, tab);
            EasyMock.expect(dataxProcessor.getTabAlias()).andReturn(new TableAliasMapper(aliasMap)).anyTimes();

            this.replay();

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


            streamScriptRun.runStream(dataxProcessor, sinkFactory, env, totalpayInfo);
            Thread.sleep(3000);


            // DBConfig dbConfig = this.getDsFactory().getDbConfig();

            //            String[] jdbcUrls = new String[1];
            //            dbConfig.vistDbURL(false, (dbName, dbHost, jdbcUrl) -> {
            //                jdbcUrls[0] = jdbcUrl;
            //            });
            if (createDDL != null) {
                final CreateTableSqlBuilder.CreateDDL ddl = createDDL;
                this.getDsFactory().visitFirstConnection((c) -> {
                    Connection conn = c.getConnection();
                    bizVerify(conn, flinkTestCases, ddl);
                });
            }


            this.verifyAll();
        } catch (Throwable e) {
            //  Thread.sleep(4000);
            throw new RuntimeException(e);
        }
    }


    /**
     * 到目标端数据库中验证最终数据是否正确
     *
     * @param conn
     * @param ddl
     * @throws SQLException
     */
    protected void bizVerify(Connection conn, List<FlinkTestCase> flinkTestCases, CreateDDL ddl) throws SQLException {

        try (Statement statement = conn.createStatement()) {
            for (FlinkTestCase testCase : flinkTestCases) {
                testCase.verifyRelevantRow(ddl, statement);
            }
        }
    }


    public abstract class IStreamScriptRun {

        protected abstract void runStream(DataxProcessor dataxProcessor, ChunjunSinkFactory sinkFactory, StreamExecutionEnvironment env,
                                          SelectedTab selectedTab) throws Exception;

        private void verifyReocrdVals(CreateDDL ddl, Statement statement, String pk, int colNumVal, String updateTimeVal) throws SQLException {
            final String selectSQL = ddl.getSelectAllScript();
            try (ResultSet resultSet = statement.executeQuery(selectSQL + " where " + colId + "='" + pk + "'")) {
                if (resultSet.next()) {
                    // 添加且被更新了
                    IResultRows.printRow(resultSet);
                    assertResultSetFromStore(resultSet);

                    Assert.assertEquals("val of " + colEntityId, colEntityIdVal, resultSet.getString(colEntityId));
                    Assert.assertEquals("val of " + colNum, colNumVal, resultSet.getInt(colNum));
                    Assert.assertEquals("val of " + colId, pk, resultSet.getString(colId));
                    Assert.assertEquals("val of " + colCreateTime, colCreateTimeVal, resultSet.getLong(colCreateTime));
                    SimpleDateFormat datetimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    Assert.assertEquals("val of " + updateTime, updateTimeVal, datetimeFormat.format(resultSet.getTimestamp(updateTime)));
                    Assert.assertEquals("val of " + starTime, starTimeVal, datetimeFormat.format(resultSet.getTimestamp(starTime)));

                    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
                    Assert.assertEquals("val of " + updateDate, updateDateVal, dateFormat.format(resultSet.getDate(updateDate)));
                    Assert.assertEquals("val of " + price, priceVal, resultSet.getBigDecimal(price));

                } else {
                    Assert.fail("have not find row with id=" + pk);
                }
            }
        }


        public List<FlinkTestCase> createFlinkTestCases() {
            List<FlinkTestCase> testCases = Lists.newArrayList();
            /**
             * 创建一条更新记录
             */
            testCases.add(new FlinkTestCase() {
                @Override
                public List<DTO> createTestData() {
                    return createDTOsForOneSingleRecord(false);
                }

                @Override
                public void verifyRelevantRow(CreateDDL ddl, Statement statement) throws SQLException {
                    verifyReocrdVals(ddl, statement, pk, colNumValUpdated, updateTimeValUpdated);
                }
            });

            /**
             * 创建一条添加记录
             */
            testCases.add(new FlinkTestCase() {
                @Override
                public List<DTO> createTestData() {
                    DTO newAdd = createDTO(DTO.EventType.ADD, (after) -> {
                        after.put(colId, newAddedRecord);
                    });
                    return Lists.newArrayList(newAdd);
                }

                @Override
                public void verifyRelevantRow(CreateDDL ddl, Statement statement) throws SQLException {
                    String selectSQL = ddl.getSelectAllScript();
                    try (ResultSet resultSet = statement.executeQuery(selectSQL)) {
                        while (resultSet.next()) {
                            System.out.println("----->" + resultSet.getString(colId));
                        }
                    }


                    verifyReocrdVals(ddl, statement, newAddedRecord, colNumVal, updateTimeVal);
                }
            });

            /**
             * 创建三条event，最终该记录会被删除
             */
            testCases.add(new FlinkTestCase() {
                @Override
                public List<DTO> createTestData() {
                    List<DTO> deleteFinally = createDTOsForOneSingleRecord(true);
                    Map<String, Object> after;
                    Map<String, Object> before;
                    for (DTO d : deleteFinally) {
                        after = d.getAfter();
                        before = d.getBefore();
                        if (MapUtils.isNotEmpty(after)) {
                            after.put(colId, deleteFinallyId);
                        }
                        if (MapUtils.isNotEmpty(before)) {
                            before.put(colId, deleteFinallyId);
                        }
                        // 三条： 1.添加 2.更新 3.删除
                    }
                    return deleteFinally;
                }

                @Override
                public void verifyRelevantRow(CreateDDL ddl, Statement statement) throws SQLException {

                    final String countSQL = ddl.getCountSelectScript(Optional.of(colId + "='" + deleteFinallyId + "'"));
                    try (ResultSet resultSet = statement.executeQuery(countSQL)) {
                        if (resultSet.next()) {
                            Assert.assertEquals("result count must be 0", 0, resultSet.getInt(1));
                        } else {
                            Assert.fail("must contain value for SQL:" + countSQL);
                        }
                    }
                }
            });

            return testCases;
        }
    }

    protected Pair<DTOStream, ReaderSource<DTO>> createReaderSource(IStreamScriptRun streamScriptRun, StreamExecutionEnvironment env,
                                                                    TableAlias tableAlia) {
        return createReaderSource(env, tableAlia, streamScriptRun);
    }

    protected Pair<DTOStream, ReaderSource<DTO>> createReaderSource(
            StreamExecutionEnvironment env,
            TableAlias tableAlia, IStreamScriptRun streamScriptRun) {
        List<DTO> testRecords = Lists.newArrayList();
        for (FlinkTestCase testCase : streamScriptRun.createFlinkTestCases()) {
            for (DTO dto : testCase.createTestData()) {
                testRecords.add(dto);
            }
        }

        DTOStream sourceStream = DTOStream.createDispatched(tableAlia.getFrom());
        ReaderSource<DTO> readerSource = ReaderSource.createDTOSource("testStreamSource",
                env.fromElements(testRecords.toArray(new DTO[testRecords.size()])).setParallelism(1));
        readerSource.getSourceStream(env, new Tab2OutputTag<>(Collections.singletonMap(tableAlia, sourceStream)));
        return Pair.of(sourceStream, readerSource);
    }

    protected void assertResultSetFromStore(ResultSet resultSet) throws SQLException {
        //  Assert.assertEquals("val of " + colNum, colNumValUpdated, resultSet.getInt(colNum));
        Assert.assertNotNull(resultSet.getTimestamp(updateTime));
        Assert.assertNotNull(resultSet.getTimestamp(starTime));
    }

    protected DataxReader createDataxReader() {
        return mock("dataxReader", DataxReader.class);
    }


    protected SelectedTab createSelectedTab() {
        SinkTabPropsExtends sinkExt = new SinkTabPropsExtends();
        sinkExt.tabName = tableName;


        //  EasyMock.expect(sinkExt.tabName).andReturn(tableName).times(2);
        //InsertType updateMode = new InsertType();

        UpdateMode updateMode = createIncrMode();
        // EasyMock.expect(sinkExt.getIncrMode()).andReturn(updateMode);
        sinkExt.incrMode = updateMode;
        // sinkExt.uniqueKey =
        List<CMeta> metaCols = Lists.newArrayList();
        CMeta cm = new CMeta();
        cm.setName(colEntityId);
        cm.setType(DataType.create(Types.VARCHAR, "VARCHAR", 6));
        metaCols.add(cm);

        cm = new CMeta();
        cm.setName(colNum);
        cm.setType(new DataType(JDBCTypes.INTEGER));
        metaCols.add(cm);

        cm = new CMeta();
        cm.setName(colId);
        cm.setType(DataType.create(Types.VARCHAR, "VARCHAR", 32));
        cm.setPk(true);
        metaCols.add(cm);

        cm = new CMeta();
        cm.setName(colCreateTime);
        cm.setType(DataType.create(Types.BIGINT, "bigint", 8));
        metaCols.add(cm);

        cm = createUpdateTime();
        metaCols.add(cm);

        cm = new CMeta();
        cm.setName(updateDate);
        cm.setType(new DataType(JDBCTypes.DATE));
        metaCols.add(cm);

        cm = new CMeta();
        cm.setName(starTime);
        cm.setType(new DataType(JDBCTypes.TIMESTAMP));
        metaCols.add(cm);

        cm = new CMeta();
        cm.setName(price);
        DataType decimal = DataType.create(Types.DECIMAL, "decimal", 10);
        decimal.setDecimalDigits(2);
        cm.setType(decimal);
        metaCols.add(cm);


        SelectedTab totalpayInfo = createSelectedTab(metaCols);
        totalpayInfo.setIncrSinkProps(sinkExt);
        totalpayInfo.name = tableName;
        totalpayInfo.primaryKeys = getUniqueKey(metaCols);
        return totalpayInfo;
    }


    protected SelectedTab createSelectedTab(List<CMeta> metaCols) {
        SelectedTab tab = new SelectedTab();
        tab.cols.addAll(metaCols);
        return tab;
    }

    protected List<String> getUniqueKey(List<CMeta> metaCols) {
        return (metaCols.stream().filter((col) -> col.isPk()).map((col) -> col.getName()).collect(Collectors.toList()));//.collect(Collectors.toList());
        //  return Lists.newArrayList(colId, updateTime);
    }

    protected CMeta createUpdateTime() {
        CMeta cm;
        cm = new CMeta();
        cm.setName(updateTime);
        // cm.setPk(true);
        cm.setType(DataType.getType(JDBCTypes.TIMESTAMP));
        return cm;
    }


    protected UpdateMode createIncrMode() {
        ReplaceType updateMode = new ReplaceType();
        //  updateMode.updateKey = Lists.newArrayList(colId, updateTime);
        return updateMode;
    }

    protected abstract ChunjunSinkFactory getSinkFactory();

    protected abstract DataxWriter createDataXWriter();

    protected DTO createDTO(DTO.EventType eventType, Consumer<Map<String, Object>>... consumer) {
        DTO d = new DTO();
        d.setEventType(eventType);
        d.setTableName(tableName);
        Map<String, Object> after = Maps.newHashMap();
        after.put(colEntityId, colEntityIdVal);
        after.put(colNum, colNumVal);
        after.put(colId, pk);
        after.put(colCreateTime, colCreateTimeVal);
        //  after.put(updateTime, "2021-12-17T09:21:20Z");
        after.put(updateTime, updateTimeVal);
        after.put(starTime, starTimeVal);
        after.put(updateDate, updateDateVal);
        after.put(price, priceVal);
        d.setAfter(after);
        if (eventType != DTO.EventType.ADD || consumer.length > 0) {
            d.setBefore(Maps.newHashMap(after));
            for (Consumer<Map<String, Object>> c : consumer) {
                c.accept(after);
            }
        }
        return d;
    }
}
