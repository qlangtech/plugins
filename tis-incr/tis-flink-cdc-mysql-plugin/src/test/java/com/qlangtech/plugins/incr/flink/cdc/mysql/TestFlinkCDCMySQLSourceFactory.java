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

package com.qlangtech.plugins.incr.flink.cdc.mysql;

import com.google.common.collect.Lists;
import com.qlangtech.plugins.incr.flink.cdc.CUDCDCTestSuit;
import com.qlangtech.plugins.incr.flink.cdc.IResultRows;
import com.qlangtech.plugins.incr.flink.cdc.RowVals;
import com.qlangtech.plugins.incr.flink.cdc.TestRow;
import com.qlangtech.plugins.incr.flink.cdc.source.TestTableRegisterFlinkSourceHandle;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.async.message.client.consumer.IMQListener;
import com.qlangtech.tis.async.message.client.consumer.MQConsumeException;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.manage.common.CenterResource;
import com.qlangtech.tis.manage.common.TisUTF8;
import com.qlangtech.tis.plugin.datax.common.BasicDataXRdbmsReader;
import com.qlangtech.tis.plugin.ds.BasicDataSourceFactory;
import com.qlangtech.tis.plugin.ds.DataType;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.test.TISEasyMock;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

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


    @Test
    public void testBinlogConsume() throws Exception {
        FlinkCDCMySQLSourceFactory mysqlCDCFactory = new FlinkCDCMySQLSourceFactory();
        mysqlCDCFactory.startupOptions = "latest";
        final String tabName = "base";

        CUDCDCTestSuit cdcTestSuit = new CUDCDCTestSuit() {
            @Override
            protected BasicDataSourceFactory createDataSourceFactory(TargetResName dataxName) {
                return createMySqlDataSourceFactory(dataxName);
            }

            @Override
            protected String getColEscape() {
                return "`";
            }
        };

        cdcTestSuit.startTest(mysqlCDCFactory, tabName);

    }

    @Test
    public void testBinlogConsumeWithDataStreamRegisterTable() throws Exception {
        FlinkCDCMySQLSourceFactory mysqlCDCFactory = new FlinkCDCMySQLSourceFactory();
        mysqlCDCFactory.startupOptions = "latest";
        final String tabName = "base";

        CUDCDCTestSuit cdcTestSuit = new CUDCDCTestSuit() {
            @Override
            protected BasicDataSourceFactory createDataSourceFactory(TargetResName dataxName) {
                return createMySqlDataSourceFactory(dataxName);
            }

            @Override
            protected String getColEscape() {
                return "`";
            }

            @Override
            protected IResultRows createConsumerHandle(String tabName) {
                return new TestTableRegisterFlinkSourceHandle(tabName, cols);
            }
        };

        cdcTestSuit.startTest(mysqlCDCFactory, tabName);

    }

    /**
     * 测试 instancedetail
     *
     * @throws Exception
     */
    @Test
    public void testBinlogConsumeWithDataStreamRegisterInstaneDetailTable() throws Exception {
        FlinkCDCMySQLSourceFactory mysqlCDCFactory = new FlinkCDCMySQLSourceFactory();
        mysqlCDCFactory.startupOptions = "latest";
        final String tabName = "instancedetail";


        CUDCDCTestSuit cdcTestSuit = new CUDCDCTestSuit() {
            @Override
            protected BasicDataSourceFactory createDataSourceFactory(TargetResName dataxName) {
                return createMySqlDataSourceFactory(dataxName);
            }

            @Override
            protected String getColEscape() {
                return "`";
            }

            @Override
            protected IResultRows createConsumerHandle(String tabName) {
                return new TestTableRegisterFlinkSourceHandle(tabName, cols);
            }

            @Override
            protected void verfiyTableCrudProcess(String tabName, BasicDataXRdbmsReader dataxReader
                    , ISelectedTab tab, IResultRows consumerHandle, IMQListener<JobExecutionResult> imqListener)
                    throws MQConsumeException, InterruptedException {
               // super.verfiyTableCrudProcess(tabName, dataxReader, tab, consumerHandle, imqListener);

                List<ISelectedTab> tabs = Collections.singletonList(tab);

                List<TestRow> exampleRows = Lists.newArrayList();
                exampleRows.add(this.parseTestRow(RowKind.INSERT, TestFlinkCDCMySQLSourceFactory.class, tabName + "/insert1.txt"));

                Assert.assertEquals(1, exampleRows.size());
                imqListener.start(dataxName, dataxReader, tabs, null);

                Thread.sleep(1000);
                CloseableIterator<Row> snapshot = consumerHandle.getRowSnapshot(tabName);
                BasicDataSourceFactory dataSourceFactory = (BasicDataSourceFactory) dataxReader.getDataSourceFactory();
                Assert.assertNotNull("dataSourceFactory can not be null", dataSourceFactory);
                dataSourceFactory.visitFirstConnection((conn) -> {
                    startProcessConn(conn);

                    for (TestRow t : exampleRows) {
                        RowVals<Object> vals = t.vals;
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
                            col.type.accept(new DataType.TypeVisitor<Void>() {
                                @Override
                                public Void longType(DataType type) {
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

                        statement.close();
                        sleepForAWhile();

                        System.out.println("wait to show insert rows");
                        waitForSnapshotStarted(snapshot);

                        List<TestRow> rows = fetchRows(snapshot, 1, false);
                        for (TestRow rr : rows) {
                            System.out.println("------------" + rr.get("instance_id"));
                            assertTestRow(tabName, RowKind.INSERT, consumerHandle, t, rr);
                        }

                    }
                });
            }
        };

        cdcTestSuit.startTest(mysqlCDCFactory, tabName);
    }


    protected BasicDataSourceFactory createMySqlDataSourceFactory(TargetResName dataxName) {
        Descriptor mySqlV5DataSourceFactory = TIS.get().getDescriptor("MySQLV5DataSourceFactory");
        Assert.assertNotNull(mySqlV5DataSourceFactory);

        Descriptor.FormData formData = new Descriptor.FormData();
        formData.addProp("name", "mysql");
        formData.addProp("dbName", MYSQL_CONTAINER.getDatabaseName());
        formData.addProp("nodeDesc", MYSQL_CONTAINER.getHost());
        formData.addProp("password", MYSQL_CONTAINER.getPassword());
        formData.addProp("userName", MYSQL_CONTAINER.getUsername());
        formData.addProp("port", String.valueOf(MYSQL_CONTAINER.getDatabasePort()));
        formData.addProp("encode", "utf8");
        formData.addProp("useCompression", "true");

        Descriptor.ParseDescribable<BasicDataSourceFactory> parseDescribable
                = mySqlV5DataSourceFactory.newInstance(dataxName.getName(), formData);
        Assert.assertNotNull(parseDescribable.instance);

        return parseDescribable.instance;
    }
}
