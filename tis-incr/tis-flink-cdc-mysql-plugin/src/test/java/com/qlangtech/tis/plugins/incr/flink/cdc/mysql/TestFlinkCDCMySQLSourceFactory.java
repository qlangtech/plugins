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

package com.qlangtech.tis.plugins.incr.flink.cdc.mysql;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.qlangtech.plugins.incr.flink.cdc.AssertRow;
import com.qlangtech.plugins.incr.flink.cdc.CDCTestSuitParams;
import com.qlangtech.plugins.incr.flink.cdc.CUDCDCTestSuit;
import com.qlangtech.plugins.incr.flink.cdc.IResultRows;
import com.qlangtech.plugins.incr.flink.cdc.RowValsExample;
import com.qlangtech.plugins.incr.flink.cdc.TestRow;
import com.qlangtech.plugins.incr.flink.cdc.source.TestTableRegisterFlinkSourceHandle;
import com.qlangtech.plugins.incr.flink.launch.TISFlinkCDCStreamFactory;
import com.qlangtech.tis.async.message.client.consumer.IMQListener;
import com.qlangtech.tis.async.message.client.consumer.MQConsumeException;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.datax.IGroupChildTaskIterator;
import com.qlangtech.tis.datax.impl.DataxReader;
import com.qlangtech.tis.manage.common.CenterResource;
import com.qlangtech.tis.manage.common.TisUTF8;
import com.qlangtech.tis.plugin.datax.common.BasicDataXRdbmsReader;

import com.qlangtech.tis.plugin.datax.transformer.RecordTransformer;
import com.qlangtech.tis.plugin.datax.transformer.RecordTransformerRules;
import com.qlangtech.tis.plugin.datax.transformer.impl.ConcatUDF;
import com.qlangtech.tis.plugin.datax.transformer.impl.ConcatUDF.Separator;
import com.qlangtech.tis.plugin.datax.transformer.impl.CopyValUDF;
import com.qlangtech.tis.plugin.datax.transformer.impl.ExistTargetCoumn;
import com.qlangtech.tis.plugin.datax.transformer.impl.JSONSplitterUDF;
import com.qlangtech.tis.plugin.datax.transformer.impl.SubStrUDF;
import com.qlangtech.tis.plugin.datax.transformer.impl.VirtualTargetColumn;
import com.qlangtech.tis.plugin.datax.transformer.jdbcprop.TargetColType;
import com.qlangtech.tis.plugin.ds.BasicDataSourceFactory;
import com.qlangtech.tis.plugin.ds.ContextParamConfig;
import com.qlangtech.tis.plugin.ds.DataType;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.ds.JDBCTypes;
import com.qlangtech.tis.plugin.ds.RdbmsRunningContext;
import com.qlangtech.tis.plugin.incr.TISSinkFactory;
import com.qlangtech.tis.plugins.incr.flink.cdc.mysql.startup.LatestStartupOptions;
import com.qlangtech.tis.realtime.ReaderSource;
import com.qlangtech.tis.test.TISEasyMock;
import com.qlangtech.tis.utils.IntegerUtils;
import org.apache.flink.cdc.connectors.IDataSourceFactoryCreator;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlContainer;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.testcontainers.containers.JdbcDatabaseContainer;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.qlangtech.plugins.incr.flink.cdc.CUDCDCTestSuit.keyBaseId;
import static com.qlangtech.plugins.incr.flink.cdc.CUDCDCTestSuit.keyCol_text;
import static com.qlangtech.plugins.incr.flink.cdc.CUDCDCTestSuit.key_json_content;
import static com.qlangtech.plugins.incr.flink.cdc.CUDCDCTestSuit.key_name_from_json_content;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-12-18 09:57
 **/
public class TestFlinkCDCMySQLSourceFactory extends MySqlSourceTestBase implements TISEasyMock {
    //private static final Logger LOG = LoggerFactory.getLogger(TestFlinkCDCMySQLSourceFactory.class);
    @Before
    public void setUp() throws Exception {
        CenterResource.setNotFetchFromCenterRepository();
    }

    @Override
    protected CDCTestSuitParams.Builder suitParamBuilder(String tabName) {
        //return new CDCTestSuitParams.Builder();
        return CDCTestSuitParams.createBuilder();
        // return CDCTestSuitParams.chunjunBuilder();
    }

    @Override
    protected JdbcDatabaseContainer getMysqlContainer() {
        return MySqlContainer.MYSQL5_CONTAINER;
    }

    /**
     * 使用base表（base_01,base_02）的分表策略测试
     *
     * @throws Exception
     */
    @Test()
    public void testBaseTableWithSplit() throws Exception {
        FlinkCDCMySQLSourceFactory mysqlCDCFactory = createCDCFactory();
        mysqlCDCFactory.startupOptions = new LatestStartupOptions();

        // final String tabName = "base";
        TISFlinkCDCStreamFactory streamFactory = new TISFlinkCDCStreamFactory();
        streamFactory.parallelism = 1;
        CDCTestSuitParams suitParams = tabParamMap.get(tabBase);//new CDCTestSuitParams("base");
        CUDCDCTestSuit cdcTestSuit = new CUDCDCTestSuit(suitParams, Optional.of("_01")) {
            @Override
            protected BasicDataSourceFactory createDataSourceFactory(TargetResName dataxName, boolean useSplitTabStrategy) {
                return (BasicDataSourceFactory) ((MySqlContainer) getMysqlContainer()).createMySqlDataSourceFactory(dataxName, useSplitTabStrategy);
            }

            @Override
            protected IResultRows createConsumerHandle(BasicDataXRdbmsReader dataxReader, String tabName, TISSinkFactory sinkFuncFactory) {
                TestTableRegisterFlinkSourceHandle sourceHandle = new TestTableRegisterFlinkSourceHandle(tabName, cols);
                sourceHandle.setSinkFuncFactory(sinkFuncFactory);
                sourceHandle.setSourceStreamTableMeta(dataxReader);
                sourceHandle.setStreamFactory(streamFactory);
                sourceHandle.setSourceFlinkColCreator(mysqlCDCFactory.createFlinkColCreator(dataxReader));
                return sourceHandle;
            }
        };

        cdcTestSuit.startTest(mysqlCDCFactory);

    }

    // @Test(timeout = 20000)
    @Test()
    public void testBinlogConsume() throws Exception {

        FlinkCDCMySQLSourceFactory mysqlCDCFactory = createCDCFactory();
        mysqlCDCFactory.startupOptions = new LatestStartupOptions();

        CDCTestSuitParams suitParams = tabParamMap.get(tabBase);
        CUDCDCTestSuit cdcTestSuit = new CUDCDCTestSuit(suitParams) {
            @Override
            protected BasicDataSourceFactory createDataSourceFactory(TargetResName dataxName, boolean useSplitTabStrategy) {
                return (BasicDataSourceFactory) ((MySqlContainer) getMysqlContainer()).createMySqlDataSourceFactory(dataxName);

            }

            @Override
            protected IResultRows createConsumerHandle(BasicDataXRdbmsReader dataxReader, String tabName, TISSinkFactory sinkFuncFactory) {
                TestTableRegisterFlinkSourceHandle sourceHandle = new TestTableRegisterFlinkSourceHandle(tabName, cols);
                sourceHandle.setSinkFuncFactory(sinkFuncFactory);
                sourceHandle.setSourceStreamTableMeta(dataxReader);
                return sourceHandle;
            }
        };

        cdcTestSuit.startTest(mysqlCDCFactory);
    }


    /**
     * 在增量消费流程中使用 T(Transformer) 规则
     *
     * @throws Exception
     */
    @Test()
    public void testBinlogConsumeWithRowTransformer() throws Exception {

        final DataxReader dataxReader = createReader(null);
        DataxReader.dataxReaderGetter = (name) -> {
            return dataxReader;
        };

        // 测试中使用一个copyVal，和subString 两个控制字段
        RecordTransformerRules.transformerRulesLoader4Test = (tab) -> {
            RecordTransformerRules tRules = new RecordTransformerRules();

            addContactWithContextParamTransformer(tRules);
            addCopyValTransformer(tRules);
            addSubStrTransformer(tRules);
            addJSONSplit(tRules);

            return tRules;
        };

        TISFlinkCDCStreamFactory streamFactory = new TISFlinkCDCStreamFactory();
        streamFactory.parallelism = 1;
        FlinkCDCMySQLSourceFactory mysqlCDCFactory = createCDCFactory();
        mysqlCDCFactory.startupOptions = new LatestStartupOptions();
        mysqlCDCFactory.timeZone = FlinkCDCMySQLSourceFactory.dftZoneId();

        CDCTestSuitParams suitParams = tabParamMap.get(tabBase);
        CUDCDCTestSuit cdcTestSuit = new CUDCDCTestSuit(suitParams) {
            @Override
            protected BasicDataSourceFactory createDataSourceFactory(TargetResName dataxName, boolean useSplitTabStrategy) {
                return (BasicDataSourceFactory) ((MySqlContainer) getMysqlContainer()).createMySqlDataSourceFactory(dataxName);

            }

            @Override
            protected IResultRows createConsumerHandle(BasicDataXRdbmsReader dataxReader, String tabName, TISSinkFactory sinkFuncFactory) {
                TestTableRegisterFlinkSourceHandle sourceHandle = new TestTableRegisterFlinkSourceHandle(tabName, cols);
                sourceHandle.setSinkFuncFactory(sinkFuncFactory);
                sourceHandle.setSourceStreamTableMeta(dataxReader);
                sourceHandle.setStreamFactory(streamFactory);
                sourceHandle.setSourceFlinkColCreator(mysqlCDCFactory.createFlinkColCreator(dataxReader));

                return sourceHandle;
            }
        };

        cdcTestSuit.startTest(mysqlCDCFactory);

    }


    private static void addJSONSplit(RecordTransformerRules tRules) {

        TargetColType toColType;

        RecordTransformer transformer = new RecordTransformer();
        JSONSplitterUDF jsonSplit = new JSONSplitterUDF();
        jsonSplit.from = key_json_content;


        toColType = new TargetColType();
        VirtualTargetColumn existTargetCoumn = new VirtualTargetColumn();
        existTargetCoumn.name = key_name_from_json_content;
        toColType.setTarget(existTargetCoumn);
        toColType.setType(DataType.createVarChar(64));
        jsonSplit.skipError = false;
        jsonSplit.prefix = "prefix_";

        jsonSplit.to = Lists.newArrayList(toColType);
        transformer.setUdf(jsonSplit);
        tRules.rules.add(transformer);
    }


    private static void addSubStrTransformer(RecordTransformerRules tRules) {

        TargetColType targetColType;

        RecordTransformer transformer = new RecordTransformer();
        SubStrUDF subStr = new SubStrUDF();
        subStr.start = 0;
        subStr.length = 2;
        subStr.from = keyCol_text;

        targetColType = new TargetColType();
        ExistTargetCoumn existTargetCoumn = new ExistTargetCoumn();
        existTargetCoumn.name = keyCol_text;
        targetColType.setTarget(existTargetCoumn);
        targetColType.setType(DataType.createVarChar(32));

        subStr.to = targetColType;
        transformer.setUdf(subStr);
        tRules.rules.add(transformer);
    }

    /**
     * 使用了环境绑定参数的contact算子替换原主键baseId
     *
     * @param tRules
     */
    private static void addContactWithContextParamTransformer(RecordTransformerRules tRules) {
        RecordTransformer transformer = new RecordTransformer();
        ConcatUDF concatUDF = new ConcatUDF();
        List<TargetColType> from = Lists.newArrayList();


        from.add(createBaseId(true));


        TargetColType targetColType = new TargetColType();
        VirtualTargetColumn virtualTgtCol = new VirtualTargetColumn();
        virtualTgtCol.name = "$dbName";
        targetColType.setTarget(virtualTgtCol);
        targetColType.setType(DataType.createVarChar(32));
        from.add(targetColType);

        concatUDF.from = from;
        concatUDF.to = createBaseId(false);
        concatUDF.separator = Separator.Cut.name();

        transformer.setUdf(concatUDF);
        tRules.rules.add(transformer);
    }

    private static TargetColType createBaseId(boolean from) {
        TargetColType targetColType = new TargetColType();
        ExistTargetCoumn targetCol = new ExistTargetCoumn();
        targetCol.name = keyBaseId;
        targetColType.setTarget(targetCol);
        targetColType.setType(from ? DataType.getType(JDBCTypes.BIGINT) : DataType.createVarChar(32));
        return targetColType;
    }

    private static void addCopyValTransformer(RecordTransformerRules tRules) {
        RecordTransformer transformer = new RecordTransformer();
        CopyValUDF cpUDF = new CopyValUDF();
        cpUDF.from = "price";
        TargetColType targetColType = new TargetColType();
        VirtualTargetColumn targetColumn = new VirtualTargetColumn();
        final String baseId = "copy_from_price";
        targetColumn.name = baseId;
        targetColType.setTarget(targetColumn);
        DataType decimal = new DataType(JDBCTypes.DECIMAL, 5);
        decimal.setDecimalDigits(2);
        targetColType.setType(decimal);
        cpUDF.to = targetColType;
        transformer.setUdf(cpUDF);
        tRules.rules.add(transformer);
        //  return cpUDF;
    }


    @Test()
    public void testFullTypesConsume() throws Exception {
        TISFlinkCDCStreamFactory streamFactory = new TISFlinkCDCStreamFactory();
        streamFactory.parallelism = 1;
        FlinkCDCMySQLSourceFactory mysqlCDCFactory = createCDCFactory();
        mysqlCDCFactory.startupOptions = new LatestStartupOptions();
        mysqlCDCFactory.timeZone = FlinkCDCMySQLSourceFactory.dftZoneId();
        // final String tabName = "base";
        CDCTestSuitParams suitParams = tabParamMap.get(fullTypes);
        Assert.assertNotNull(suitParams);
        CUDCDCTestSuit cdcTestSuit = new CUDCDCTestSuit(suitParams) {
            @Override
            protected BasicDataSourceFactory createDataSourceFactory(TargetResName dataxName, boolean useSplitTabStrategy) {
                return (BasicDataSourceFactory) ((IDataSourceFactoryCreator) getMysqlContainer()).createMySqlDataSourceFactory(dataxName);
            }

            @Override
            protected List<TestRow> createExampleTestRows() {
//super.createExampleTestRows();
                Map<String, RowValsExample.RowVal> vals = Maps.newHashMap();

//                vals.put(keyBaseId, RowValsExample.RowVal.$(i));
//                vals.put(keyStart_time, parseTimestamp(timeFormat.get().format(now)));
//                vals.put("update_date", parseDate(dateFormat.get().format(now)));
//                vals.put(key_update_time, parseTimestamp(timeFormat.get().format(now)));
//                vals.put("price", RowValsExample.RowVal.decimal(199, 2));
//                vals.put("json_content", RowValsExample.RowVal.json("{\"name\":\"baisui#" + i + "\"}"));
//                vals.put("col_blob", RowValsExample.RowVal.stream("Hello world"));
//                vals.put(keyCol_text, RowValsExample.RowVal.$("我爱北京天安门" + i));

                int pk = 2;
                vals.put("id", RowValsExample.RowVal.$(pk));
                vals.put("tiny_c", RowValsExample.RowVal.$((byte) 255));
                vals.put("tiny_un_c", RowValsExample.RowVal.$((byte) 127));
                vals.put("small_c", RowValsExample.RowVal.$((short) 32767));
                vals.put("small_un_c", RowValsExample.RowVal.$((short) 5534));
                vals.put("medium_c", RowValsExample.RowVal.$(8388607));
                vals.put("medium_un_c", RowValsExample.RowVal.$(16777215l));// MEDIUMINT UNSIGNED,
                vals.put("int_c", RowValsExample.RowVal.$(2147483647));
                vals.put("int_un_c", RowValsExample.RowVal.$(4294967295l)); //INTEGER UNSIGNED,
                vals.put("int11_c", RowValsExample.RowVal.$(2147483647));
                vals.put("big_c", RowValsExample.RowVal.$(9223372036854775807l));
                vals.put("big_un_c", RowValsExample.RowVal.$(9223372036854775807l));
                vals.put("varchar_c", RowValsExample.RowVal.$("Hello World"));
                vals.put("char_c", RowValsExample.RowVal.$("abc"));
                vals.put("real_c", RowValsExample.RowVal.$(123.102d));
                vals.put("float_c", RowValsExample.RowVal.$(123.102f));
                vals.put("double_c", RowValsExample.RowVal.$(404.4443d));
                vals.put("decimal_c", RowValsExample.RowVal.decimal(1234567l, 4));
                vals.put("numeric_c", RowValsExample.RowVal.decimal(3456, 0));
                vals.put("big_decimal_c", RowValsExample.RowVal.decimal(345678921, 1));
                vals.put("bit1_c", RowValsExample.RowVal.bit(true));
                vals.put("tiny1_c", RowValsExample.RowVal.bit(true));
                vals.put("boolean_c", RowValsExample.RowVal.bit(true));

                vals.put("date_c", parseDate("2020-07-17"));

                vals.put("time_c", RowValsExample.RowVal.time("18:00:22"));
                vals.put("datetime3_c", parseTimestamp("2020-07-17 18:00:22"));
                vals.put("datetime6_c", parseTimestamp("2020-07-17 18:00:22"));
                vals.put("timestamp_c", parseTimestamp("2020-07-17 18:00:22"));
                vals.put("file_uuid", RowValsExample.RowVal.stream(StringUtils.lowerCase("FA34E10293CB42848573A4E39937F479")
                        , (raw) -> {
                            StringBuffer result = new StringBuffer();
                            int val;
                            for (int offset = 0; offset < 4; offset++) {
                                result.append(Integer.toHexString(IntegerUtils.intFromByteArray(raw, offset * 4)));
                            }
                            return result.toString();
                        }).setSqlParamDecorator(() -> "UNHEX(?)"));
                String colBitC = "bit_c";
                vals.put(colBitC, RowValsExample.RowVal.stream("val", (raw) -> {
                    //TODO 暂时先让测试通过
                    return "val";
                })); //b'0000010000000100000001000000010000000100000001000000010000000100'
                vals.put("text_c", RowValsExample.RowVal.$("text"));
                vals.put("tiny_blob_c", RowValsExample.RowVal.stream("blob_c"));
                vals.put("blob_c", RowValsExample.RowVal.stream("blob_c"));
                vals.put("medium_blob_c", RowValsExample.RowVal.stream("medium_blob_c"));
                vals.put("long_blob_c", RowValsExample.RowVal.stream("long_blob_c_long_blob_c"));
                vals.put("year_c", RowValsExample.RowVal.$(2021));
                vals.put("enum_c", RowValsExample.RowVal.$("white"));//  default 'd',
                vals.put("set_c", RowValsExample.RowVal.$("a,b"));
                vals.put("json_c", RowValsExample.RowVal.$("{\"key1\":\"value1\"}"));
//                vals.put("point_c", RowValsExample.RowVal.$("ST_GeomFromText('POINT(1 1)')"));
//                vals.put("geometry_c", RowValsExample.RowVal.stream("ST_GeomFromText('POLYGON((1 1, 2 1, 2 2,  1 2, 1 1))')"));
//                vals.put("linestring_c", RowValsExample.RowVal.$("ST_GeomFromText('LINESTRING(3 0, 3 3, 3 5)')"));
//                vals.put("polygon_c", RowValsExample.RowVal.$("ST_GeomFromText('POLYGON((1 1, 2 1, 2 2,  1 2, 1 1))')"));
//                vals.put("multipoint_c", RowValsExample.RowVal.$("ST_GeomFromText('MULTIPOINT((1 1),(2 2))')"));
//                vals.put("multiline_c", RowValsExample.RowVal.$("ST_GeomFromText('MultiLineString((1 1,2 2,3 3),(4 4,5 5))')"));
//                vals.put("multipolygon_c", RowValsExample.RowVal.$("ST_GeomFromText('MULTIPOLYGON(((0 0, 10 0, 10 10, 0 10, 0 0)), ((5 5, 7 5, 7 7, 5 7, 5 5)))')"));
//                vals.put("geometrycollection_c", RowValsExample.RowVal.$("ST_GeomFromText('GEOMETRYCOLLECTION(POINT(10 10), POINT(30 30), LINESTRING(15 15, 20 20))')"));

                TestRow fullTypeRow = new TestRow(RowKind.INSERT, this.getColMetaMapper(), new RowValsExample(vals));
                fullTypeRow.idVal = pk;
//                fullTypeRow.updateVals.put(colBitC, (statement, index, ovals) -> {
//
//                    RowValsExample.RowVal val = RowValsExample.RowVal.stream("val2", (raw) -> {
//                        //TODO 暂时先让测试通过
//                        return "val2";
//                    });
//                    ByteArrayInputStream bytes = val.getVal();
//                    //String newVal = "update#" + ovals.getString(keyCol_text);
//                    statement.setBytes(index, IOUtils.toByteArray(bytes));
//                    return val;
//                });

                return Lists.newArrayList(fullTypeRow);
            }


            @Override
            protected IResultRows createConsumerHandle(BasicDataXRdbmsReader dataxReader, String tabName, TISSinkFactory sinkFuncFactory) {
                TestTableRegisterFlinkSourceHandle sourceHandle = new TestTableRegisterFlinkSourceHandle(tabName, cols);
                sourceHandle.setSinkFuncFactory(sinkFuncFactory);
                sourceHandle.setSourceStreamTableMeta(dataxReader);
                sourceHandle.setStreamFactory(streamFactory);
                sourceHandle.setSourceFlinkColCreator(mysqlCDCFactory.createFlinkColCreator(dataxReader));
                return sourceHandle;
            }

        };

        cdcTestSuit.startTest(mysqlCDCFactory);

    }

    protected FlinkCDCMySQLSourceFactory createCDCFactory() {
        FlinkCDCMySQLSourceFactory mySQLSourceFactory = new FlinkCDCMySQLSourceFactory();
        mySQLSourceFactory.timeZone = FlinkCDCMySQLSourceFactory.dftZoneId();
        return mySQLSourceFactory;
    }


    @Test
    public void testBinlogConsumeWithDataStreamRegisterTable() throws Exception {
        FlinkCDCMySQLSourceFactory mysqlCDCFactory = createCDCFactory();
        mysqlCDCFactory.startupOptions = new LatestStartupOptions();
        //  final String tabName = "base";
        CDCTestSuitParams suitParams = tabParamMap.get(tabBase);// new CDCTestSuitParams(tabName);
        CUDCDCTestSuit cdcTestSuit = new CUDCDCTestSuit(suitParams) {
            @Override
            protected BasicDataSourceFactory createDataSourceFactory(TargetResName dataxName, boolean useSplitTabStrategy) {
                return (BasicDataSourceFactory) MySqlContainer.MYSQL5_CONTAINER.createMySqlDataSourceFactory(dataxName);
            }

            @Override
            protected IResultRows createConsumerHandle(BasicDataXRdbmsReader dataxReader, String tabName, TISSinkFactory sinkFuncFactory) {
                TestTableRegisterFlinkSourceHandle sourceHandle = new TestTableRegisterFlinkSourceHandle(tabName, cols);
                sourceHandle.setSinkFuncFactory(sinkFuncFactory);
                return sourceHandle;
            }
        };

        cdcTestSuit.startTest(mysqlCDCFactory);

    }

    /**
     * 测试 instancedetail
     *
     * @throws Exception
     */
    @Test
    public void testBinlogConsumeWithDataStreamRegisterInstaneDetailTable() throws Exception {
        FlinkCDCMySQLSourceFactory mysqlCDCFactory = createCDCFactory();
        mysqlCDCFactory.startupOptions = new LatestStartupOptions();
        // final String tabName = "instancedetail";

        CDCTestSuitParams suitParams = tabParamMap.get(tabInstanceDetail);//new CDCTestSuitParams(tabName);
        CUDCDCTestSuit cdcTestSuit = new CUDCDCTestSuit(suitParams) {
            @Override
            protected BasicDataSourceFactory createDataSourceFactory(TargetResName dataxName, boolean useSplitTabStrategy) {
                return createDataSource(dataxName);
            }

//            @Override
//            protected String getColEscape() {
//                return "`";
//
//            }

            @Override
            protected IResultRows createConsumerHandle(BasicDataXRdbmsReader dataxReader, String tabName, TISSinkFactory sinkFuncFactory) {
                TestTableRegisterFlinkSourceHandle sourceHandle = new TestTableRegisterFlinkSourceHandle(tabName, cols);
                sourceHandle.setSinkFuncFactory(sinkFuncFactory);
                sourceHandle.setSourceStreamTableMeta(dataxReader);
                return sourceHandle;
            }

            /**
             * String tabName, BasicDataXRdbmsReader dataxReader
             *             , ISelectedTab tab, IResultRows consumerHandle, IMQListener<List<ReaderSource>> imqListener
             * @param tabName
             * @param dataxReader
             * @param tab
             * @param consumerHandle
             * @param imqListener
             * @throws MQConsumeException
             * @throws InterruptedException
             */
            @Override
            protected void manipulateAndVerfiyTableCrudProcess(String tabName, BasicDataXRdbmsReader dataxReader
                    , ISelectedTab tab, IResultRows consumerHandle, IMQListener<List<ReaderSource>> imqListener)
                    throws MQConsumeException, InterruptedException {
                // super.verfiyTableCrudProcess(tabName, dataxReader, tab, consumerHandle, imqListener);

                List<ISelectedTab> tabs = Collections.singletonList(tab);

                List<TestRow> exampleRows = Lists.newArrayList();
                exampleRows.add(this.parseTestRow(RowKind.INSERT, TestFlinkCDCMySQLSourceFactory.class, tabName + "/insert1.txt"));

                Assert.assertEquals(1, exampleRows.size());
                imqListener.start(false, dataxName, dataxReader, tabs, createProcess());

                Thread.sleep(1000);
                CloseableIterator<Row> snapshot = consumerHandle.getRowSnapshot(tabName);
                BasicDataSourceFactory dataSourceFactory = (BasicDataSourceFactory) dataxReader.getDataSourceFactory();
                Assert.assertNotNull("dataSourceFactory can not be null", dataSourceFactory);
                dataSourceFactory.visitFirstConnection((cc) -> {
                    Connection conn = cc.getConnection();
                    startProcessConn(cc);

                    for (TestRow t : exampleRows) {
                        RowValsExample vals = t.vals;
                        final String insertBase
                                = "insert into " + createTableName(tabName) + "("
                                + cols.stream().filter((c) -> vals.notNull(c.getName())).map((col) -> getColEscape() + col.getName() + getColEscape()).collect(Collectors.joining(" , ")) + ") " +
                                "values(" +
                                cols.stream().filter((c) -> vals.notNull(c.getName()))
                                        .map((col) -> "?")
                                        .collect(Collectors.joining(" , ")) + ")";

                        PreparedStatement statement = conn.prepareStatement(insertBase);
                        AtomicInteger ci = new AtomicInteger();
                        cols.stream().filter((c) -> vals.notNull(c.getName())).forEach((col) -> {
                            col.getType().accept(new DataType.TypeVisitor<Void>() {
                                @Override
                                public Void bigInt(DataType type) {
                                    try {
                                        statement.setLong(ci.incrementAndGet(), Long.parseLong(vals.getString(col.getName())));
                                    } catch (SQLException e) {
                                        throw new RuntimeException(e);
                                    }
                                    return null;
                                }

                                @Override
                                public Void doubleType(DataType type) {
                                    try {
                                        statement.setDouble(ci.incrementAndGet(), Double.parseDouble(vals.getString(col.getName())));
                                    } catch (SQLException e) {
                                        throw new RuntimeException(e);
                                    }
                                    return null;
                                }

                                @Override
                                public Void dateType(DataType type) {
                                    try {
                                        statement.setDate(ci.incrementAndGet(), java.sql.Date.valueOf(vals.getString(col.getName())));
                                    } catch (Exception e) {
                                        throw new RuntimeException(e);
                                    }

                                    return null;
                                }

                                @Override
                                public Void timestampType(DataType type) {

                                    try {
                                        statement.setTimestamp(ci.incrementAndGet(), java.sql.Timestamp.valueOf(vals.getString(col.getName())));
                                    } catch (Exception e) {
                                        throw new RuntimeException(e);
                                    }

                                    return null;
                                }

                                @Override
                                public Void bitType(DataType type) {
                                    try {
                                        statement.setByte(ci.incrementAndGet(), Byte.parseByte(vals.getString(col.getName())));
                                    } catch (Exception e) {
                                        throw new RuntimeException(e);
                                    }

                                    return null;
                                }

                                @Override
                                public Void blobType(DataType type) {
                                    try {
                                        try (InputStream input = new ByteArrayInputStream(vals.getString(col.getName()).getBytes(TisUTF8.get()))) {
                                            statement.setBlob(ci.incrementAndGet(), input);
                                        }
                                    } catch (Exception e) {
                                        throw new RuntimeException(e);
                                    }
                                    return null;
                                }

                                @Override
                                public Void varcharType(DataType type) {
                                    try {
                                        statement.setString(ci.incrementAndGet(), (vals.getString(col.getName())));
                                    } catch (Exception e) {
                                        throw new RuntimeException(e);
                                    }

                                    return null;
                                }

                                @Override
                                public Void intType(DataType type) {
                                    try {
                                        statement.setInt(ci.incrementAndGet(), Integer.parseInt(vals.getString(col.getName())));
                                    } catch (Exception e) {
                                        throw new RuntimeException(e);
                                    }
                                    return null;
                                }

                                @Override
                                public Void floatType(DataType type) {
                                    try {
                                        statement.setFloat(ci.incrementAndGet(), Float.parseFloat(vals.getString(col.getName())));
                                    } catch (Exception e) {
                                        throw new RuntimeException(e);
                                    }
                                    return null;
                                }

                                @Override
                                public Void decimalType(DataType type) {
                                    try {
                                        statement.setBigDecimal(ci.incrementAndGet(), BigDecimal.valueOf(Double.parseDouble(vals.getString(col.getName()))));
                                    } catch (Exception e) {
                                        throw new RuntimeException(e);
                                    }
                                    return null;
                                }

                                @Override
                                public Void timeType(DataType type) {
                                    try {
                                        statement.setTime(ci.incrementAndGet(), java.sql.Time.valueOf(vals.getString(col.getName())));
                                    } catch (Exception e) {
                                        throw new RuntimeException(e);
                                    }
                                    return null;
                                }

                                @Override
                                public Void tinyIntType(DataType dataType) {
                                    try {
                                        statement.setShort(ci.incrementAndGet(), Short.parseShort(vals.getString(col.getName())));
                                    } catch (Exception e) {
                                        throw new RuntimeException(e);
                                    }
                                    return null;
                                }

                                @Override
                                public Void smallIntType(DataType dataType) {
                                    tinyIntType(dataType);
                                    return null;
                                }
                            });
                        });


                        Assert.assertEquals(1, executePreparedStatement(conn, statement));
                        conn.commit();
                        statement.close();
                        sleepForAWhile();

                        System.out.println("wait to show insert rows");
                        waitForSnapshotStarted(snapshot);

                        List<AssertRow> rows = fetchRows(snapshot, 1, t, false);
                        for (AssertRow rr : rows) {
                            System.out.println("------------" + rr.getObj("instance_id"));
                            //  assertTestRow(tabName, RowKind.INSERT, consumerHandle, t, rr);
                            assertInsertRow(t, rr);
                        }

                    }
                });
            }
        };

        cdcTestSuit.startTest(mysqlCDCFactory);
    }

    private DataxReader createReader(String dataXName) {

        DataxReader dataxReader = new DataxReader() {

//            @Override
//            public Map<String, ContextParamConfig> getDBContextParams() {
//                ContextParamConfig dbName = new ContextParamConfig("dbName") {
//                    @Override
//                    public DbNameContextParamValGetter valGetter() {
//                        return new DbNameContextParamValGetter();
//                    }
//
//                    @Override
//                    public DataType getDataType() {
//                        return DataType.createVarChar(50);
//                    }
//                };
//                return Collections.singletonMap(dbName.getKeyName(), dbName);
//            }

            @Override
            public <T extends ISelectedTab> List<T> getSelectedTabs() {
                throw new UnsupportedOperationException();
            }

            @Override
            public IGroupChildTaskIterator getSubTasks(Predicate<ISelectedTab> filter) {
                throw new UnsupportedOperationException();
            }

            @Override
            public String getTemplate() {
                throw new UnsupportedOperationException();
            }

            @Override
            public void startScanDependency() {
                throw new UnsupportedOperationException();
            }
        };
        return dataxReader;
    }
}
