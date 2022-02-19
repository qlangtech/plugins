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

package com.qlangtech.plugins.incr.flink.cdc;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.qlangtech.tis.async.message.client.consumer.IMQListener;
import com.qlangtech.tis.async.message.client.consumer.impl.MQListenerFactory;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.manage.common.TisUTF8;
import com.qlangtech.tis.plugin.datax.SelectedTab;
import com.qlangtech.tis.plugin.datax.common.BasicDataXRdbmsReader;
import com.qlangtech.tis.plugin.datax.common.RdbmsReaderContext;
import com.qlangtech.tis.plugin.ds.*;
import com.qlangtech.tis.plugin.incr.TISSinkFactory;
import com.qlangtech.tis.realtime.transfer.DTO;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;
import org.junit.Assert;

import java.io.ByteArrayInputStream;
import java.math.BigDecimal;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

/**
 * CDC增量监听测试套件
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-01-19 09:27
 **/
public abstract class CUDCDCTestSuit {


    protected final TargetResName dataxName = new TargetResName("x");
    static String keyCol_text = "col_text";
    String keyStart_time = "start_time";
    static String keyBaseId = "base_id";
    String keyColBlob = "col_blob";

    public List<FlinkCol> cols = Lists.newArrayList(
            new FlinkCol(keyBaseId, DataTypes.INT())
//            , new FlinkCol(keyStart_time, DataTypes.TIMESTAMP())
//            , new FlinkCol("update_date", DataTypes.DATE())
//            , new FlinkCol("update_time", DataTypes.TIMESTAMP())

            , new FlinkCol(keyStart_time, DataTypes.STRING())
            , new FlinkCol("update_date", DataTypes.STRING())
            , new FlinkCol("update_time", DataTypes.STRING())

            , new FlinkCol("price", DataTypes.DECIMAL(5, 2))
            , new FlinkCol("json_content", DataTypes.STRING())
            // , new FlinkCol(keyColBlob, DataTypes.STRING())
            , new FlinkCol(keyColBlob, DataTypes.BYTES(), FlinkCol.Bytes())
            , new FlinkCol(keyCol_text, DataTypes.STRING()));


    static final ThreadLocal<SimpleDateFormat> timeFormat = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        }
    };

    static final ThreadLocal<SimpleDateFormat> dateFormat = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat("yyyy-MM-dd");
        }
    };


    public void startTest(MQListenerFactory cdcFactory, String tabName) throws Exception {


        TestBasicFlinkSourceHandle consumerHandle = getTestBasicFlinkSourceHandle(tabName);

        cdcFactory.setConsumerHandle(consumerHandle);

        IMQListener<JobExecutionResult> imqListener = cdcFactory.create();

        // DataxReader.IDataxReaderGetter readerGetter = mock("IDataxReaderGetter", DataxReader.IDataxReaderGetter.class);


        BasicDataXRdbmsReader dataxReader = createDataxReader(dataxName, tabName);

//        for (String tab : dataxReader.getDataSourceFactory().getTablesInDB()) {
////            if (tab.indexOf("DEBE") > -1) {
////                System.out.println(tab);
////            }
//        }

        IDataxProcessor dataXProcessor = null;

        //  replay();
        List<SelectedTab> selectedTabs = dataxReader.getSelectedTabs();
        Optional<SelectedTab> firstSelectedTab
                = selectedTabs.stream().filter((t) -> tabName.equals(t.name)).findFirst();
        Assert.assertTrue("firstSelectedTab:" + tabName + " must be present", firstSelectedTab.isPresent());


        ISelectedTab tab = firstSelectedTab.get();
        List<ISelectedTab> tabs = Collections.singletonList(tab);

        List<TestRow> exampleRows = Lists.newArrayList();
        Date now = new Date();
        TestRow row = null;
        Map<String, Object> vals = null;
        int insertCount = 5;
        for (int i = 1; i <= insertCount; i++) {
            vals = Maps.newHashMap();
            vals.put(keyBaseId, i);
            vals.put("start_time", timeFormat.get().format(now));
            vals.put("update_date", dateFormat.get().format(now));
            vals.put("update_time", timeFormat.get().format(now));
            vals.put("price", BigDecimal.valueOf(199, 2));
            vals.put("json_content", "{\"name\":\"baisui#" + i + "\"}");
            vals.put("col_blob", new ByteArrayInputStream("Hello world".getBytes(TisUTF8.get())));
            vals.put(keyCol_text, "我爱北京天安门" + i);
            row = new TestRow(RowKind.INSERT, new RowVals(vals));
            row.idVal = i;
            exampleRows.add(row);
        }

        // 执行三条更新
        row = exampleRows.get(3);
        row.updateVals.put(keyCol_text, (statement, index, ovals) -> {
            String newVal = "update#" + ovals.getString(keyCol_text);
            statement.setString(index, newVal);
            return newVal;
        });
        row.updateVals.put(keyStart_time, (statement, index, ovals) -> {
            String v = "2012-11-13 11:11:35";
            statement.setTimestamp(index, parseTimestamp(v));
            return v;
        });

        row = exampleRows.get(4);
        row.updateVals.put(keyCol_text, (statement, index, ovals) -> {
            String v = "update#" + ovals.getString(keyCol_text);
            statement.setString(index, v);
            return v;
        });
        row.updateVals.put(keyStart_time, (statement, index, ovals) -> {
            String v = "2012-11-13 11:11:35";
            statement.setTimestamp(index, parseTimestamp(v));
            return v;
        });

        row = exampleRows.get(0);
        row.updateVals.put(keyCol_text, (statement, index, ovals) -> {
            String v = "update#" + ovals.getString(keyCol_text);
            statement.setString(index, v);
            return v;
        });
        row.updateVals.put(keyStart_time, (statement, index, ovals) -> {
            String v = "2012-11-12 11:11:35";
            statement.setTimestamp(index, parseTimestamp(v));
            return v;
        });

        // 执行两条删除
        row = exampleRows.get(1);
        row.willbeDelete = true;

        row = exampleRows.get(3);
        row.willbeDelete = true;

        imqListener.start(dataxName, dataxReader, tabs, null);

        Thread.sleep(1000);


        final String insertBase
                = "insert into " + createTableName(tabName) + "("
                + cols.stream().map((col) -> getColEscape() + col.name + getColEscape()).collect(Collectors.joining(" , ")) + ") " +
                "values(" +
                cols.stream().map((col) -> "?").collect(Collectors.joining(" , ")) + ")";


        CloseableIterator<Row> snapshot = consumerHandle.getRowSnapshot(tabName);
        //insertCount
        BasicDataSourceFactory dataSourceFactory = (BasicDataSourceFactory) dataxReader.getDataSourceFactory();
        Assert.assertNotNull("dataSourceFactory can not be null", dataSourceFactory);
        dataSourceFactory.visitFirstConnection((conn) -> {
                    startProcessConn(conn);
                    PreparedStatement statement = null;
                    try {
                        // 执行添加
                        for (TestRow r : exampleRows) {

                            statement = conn.prepareStatement(insertBase);
                            statement.setInt(1, r.getInt("base_id"));
                            statement.setTimestamp(2, parseTimestamp(r.getString("start_time")));
                            statement.setDate(3
                                    , new java.sql.Date(dateFormat.get().parse(r.getString("update_date")).getTime()));
                            statement.setTimestamp(4, parseTimestamp(r.getString("update_time")));
                            statement.setBigDecimal(5, r.getBigDecimal("price"));
                            statement.setString(6, r.getString("json_content"));

                            //  statement.setBlob(7, r.getInputStream("col_blob"));

                            statement.setBinaryStream(7, r.getInputStream("col_blob"));


                            statement.setString(8, r.getString("col_text"));
                            Assert.assertEquals(1, executePreparedStatement(conn, statement));

                            statement.close();
                            sleepForAWhile();

                            waitForSnapshotStarted(snapshot);
                            List<TestRow> rows = fetchRows(snapshot, 1, false);
                            for (TestRow rr : rows) {
                                System.out.println("------------" + rr.getInt(keyBaseId));
                                assertTestRow(RowKind.INSERT, r, rr);
                            }
                            // System.out.println("########################");

                        }


                        // 执行更新
                        for (TestRow exceptRow : exampleRows) {
                            if (!exceptRow.execUpdate()) {
                                continue;
                            }

                            List<Map.Entry<String, RowValsUpdate.UpdatedColVal>> cols = exceptRow.updateVals.getCols();

                            String updateSql = String.format("UPDATE " + createTableName(tabName) + " set %s WHERE base_id=%s"
                                    , cols.stream().map((e) -> e.getKey() + " = ?").collect(Collectors.joining(",")), exceptRow.getIdVal());

                            statement = conn.prepareStatement(updateSql);

                            int colIndex = 1;
                            for (Map.Entry<String, RowValsUpdate.UpdatedColVal> col : cols) {
                                col.getValue().setPrepColVal(statement, colIndex++, exceptRow.vals);
                            }

                            Assert.assertTrue(updateSql, executePreparedStatement(conn, statement) > 0);

                            statement.close();
                            sleepForAWhile();

                            waitForSnapshotStarted(snapshot);
                            List<TestRow> rows = fetchRows(snapshot, 1, false);
                            for (TestRow rr : rows) {
                                //System.out.println("------------" + rr.getInt(keyBaseId));
                                assertTestRow(RowKind.UPDATE_AFTER, exceptRow, rr);

                            }
                        }

                        // 执行删除
                        for (TestRow r : exampleRows) {
                            if (!r.execDelete()) {
                                continue;
                            }

                            String deleteSql = String.format("DELETE FROM " + createTableName(tabName) + " WHERE base_id=%s", r.getIdVal());
                            try (Statement statement1 = conn.createStatement()) {
                                Assert.assertTrue(deleteSql, executeStatement(conn, statement1, (deleteSql)) > 0);
                                sleepForAWhile();
                                waitForSnapshotStarted(snapshot);
                                List<TestRow> rows = fetchRows(snapshot, 1, true);
                                for (TestRow rr : rows) {
                                    //System.out.println("------------" + rr.getInt(keyBaseId));
                                    assertTestRow(RowKind.DELETE, r, rr);

                                }
                            }
                        }

                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
        );


        consumerHandle.cancel();
    }

    protected String createTableName(String tabName) {
        return tabName;
    }

    protected String getColEscape() {
        return StringUtils.EMPTY;
    }

    protected int executePreparedStatement(Connection connection, PreparedStatement statement) throws SQLException {
        return statement.executeUpdate();
    }

    protected int executeStatement(Connection connection, Statement statement, String sql) throws SQLException {
        return statement.executeUpdate(sql);
    }

    protected void startProcessConn(Connection conn) throws SQLException {
        // conn.setAutoCommit(false);
    }

    protected TestBasicFlinkSourceHandle getTestBasicFlinkSourceHandle(String tabName) {
        TestBasicFlinkSourceHandle consumerHandle = createConsumerHandle(tabName);

        // PrintSinkFunction printSinkFunction = new PrintSinkFunction();
        TISSinkFactory sinkFuncFactory = new TISSinkFactory() {
            @Override
            public Map<IDataxProcessor.TableAlias, SinkFunction<DTO>> createSinkFunction(IDataxProcessor dataxProcessor) {
//                Map<IDataxProcessor.TableAlias, SinkFunction<DTO>> result = Maps.newHashMap();
//                result.put(new IDataxProcessor.TableAlias(tabName), printSinkFunction);
//                return result;
                return Collections.emptyMap();
            }
        };
        consumerHandle.setSinkFuncFactory(sinkFuncFactory);
        return consumerHandle;
    }

    protected TestBasicFlinkSourceHandle createConsumerHandle(String tabName) {
        return new TestBasicFlinkSourceHandle(tabName);
    }

    private BasicDataXRdbmsReader createDataxReader(TargetResName dataxName, String tabName) {
        BasicDataSourceFactory dataSourceFactory = createDataSourceFactory(dataxName);
        List<ColumnMetaData> tableMetadata = dataSourceFactory.getTableMetadata(tabName);

        BasicDataXRdbmsReader dataxReader = new BasicDataXRdbmsReader() {
            @Override
            protected RdbmsReaderContext createDataXReaderContext(String jobName, SelectedTab tab, IDataSourceDumper dumper) {
                return null;
            }

            @Override
            public DataSourceFactory getDataSourceFactory() {
                return dataSourceFactory;
            }
        };

        SelectedTab baseTab = new SelectedTab(tabName);
        baseTab.setCols(tableMetadata.stream().map((m) -> m.getName()).collect(Collectors.toList()));
        dataxReader.selectedTabs = Collections.singletonList(baseTab);
        return dataxReader;
    }

    protected abstract BasicDataSourceFactory createDataSourceFactory(TargetResName dataxName);

    private void assertTestRow(RowKind updateVal, TestRow expect, TestRow actual) throws Exception {
        assertEqualsInOrder(
                expect.getValsList(Optional.of(updateVal), cols, (rowVals, key, val) -> {
                    if (keyColBlob.equals(key)) {
                        ByteArrayInputStream inputStream
                                = (ByteArrayInputStream) rowVals.getInputStream(keyColBlob);
                        inputStream.reset();
                        return IOUtils.toString(inputStream, TisUTF8.get());
                    } else {
                        return val;
                    }
                }) //
                , actual.getValsList(cols, (rowVals, key, val) -> {
                    if (keyColBlob.equals(key)) {
                        byte[] buffer = (byte[]) val;
                        // buffer.reset();
                        return new String(buffer);
                    } else {
                        return val;
                    }
                }));
    }

    private Timestamp parseTimestamp(String timeLiterial) throws ParseException {
        return new Timestamp(timeFormat.get().parse(timeLiterial).getTime());
    }

    protected void sleepForAWhile() {
        try {
            Thread.sleep(50);
        } catch (Exception e) {
        }
    }


    private static void waitForSnapshotStarted(CloseableIterator<Row> iterator) throws Exception {
        while (!iterator.hasNext()) {
            System.out.println("waitForSnapshotStarted");
            Thread.sleep(100);
        }
    }

    private static List<TestRow> fetchRows(Iterator<Row> iter, int size, boolean deleteRow) {
        List<TestRow> rows = new ArrayList<>(size);
        while (size > 0 && iter.hasNext()) {
            Row row = iter.next();
            System.out.println("=========" + row.getField(keyBaseId));
            // ignore rowKind marker
            RowVals vals = new RowVals();
            for (String key : row.getFieldNames(true)) {
                vals.put(key, row.getField(key));
            }
            rows.add(new TestRow(row.getKind(), vals));
            size--;
        }
        return rows;
    }

    public static void assertEqualsInAnyOrder(List<String> expected, List<String> actual) {
        assertTrue(expected != null && actual != null);
        assertEqualsInOrder(
                expected.stream().sorted().collect(Collectors.toList()),
                actual.stream().sorted().collect(Collectors.toList()));
    }

    public static void assertEqualsInOrder(List<String> expected, List<String> actual) {
        assertTrue(expected != null && actual != null);
        assertEquals(expected.size(), actual.size());
        assertArrayEquals(expected.toArray(new String[0]), actual.toArray(new String[0]));
    }
}
