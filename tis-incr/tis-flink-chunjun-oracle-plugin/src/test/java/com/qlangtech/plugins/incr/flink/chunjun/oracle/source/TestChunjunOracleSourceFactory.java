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

package com.qlangtech.plugins.incr.flink.chunjun.oracle.source;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.qlangtech.plugins.incr.flink.cdc.*;
import com.qlangtech.plugins.incr.flink.chunjun.oracle.sink.TestChunjunOracleSinkFactory;
import com.qlangtech.plugins.incr.flink.junit.TISApplySkipFlinkClassloaderFactoryCreation;
import com.qlangtech.tis.async.message.client.consumer.IMQListener;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.manage.common.TisUTF8;
import com.qlangtech.tis.plugin.datax.common.BasicDataXRdbmsReader;
import com.qlangtech.tis.plugin.ds.BasicDataSourceFactory;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.ds.oracle.OracleDataSourceFactory;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.types.RowKind;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TestRule;

import java.io.InputStream;
import java.net.URL;
import java.sql.Statement;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.junit.Assert.assertNotNull;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-08-24 15:33
 **/
public class TestChunjunOracleSourceFactory {
    static OracleDataSourceFactory oracleDS;
    private static final String tabNameFull_types = "full_types";
    private static final String tabNameFull_types_pk = "id";
    private static final String key_timestamp6_c = "timestamp6_c";
    private static final String key_bytea_c = "bytea_c";
    @ClassRule(order = 100)
    public static TestRule name = new TISApplySkipFlinkClassloaderFactoryCreation();
    private static final int BIG_DECIMAL_SCALA = 5;
    private static final Pattern COMMENT_PATTERN = Pattern.compile("^(.*)--.*$");

    @BeforeClass
    public static void initialize() throws Exception {
        TestChunjunOracleSinkFactory.initialize();
        oracleDS = Objects.requireNonNull(TestChunjunOracleSinkFactory.oracleDS, "oracleDS can not be null");
        initializeOracleTable("column_type_test");
        List<String> tables = oracleDS.getTablesInDB();
        String full_types = "full_types";
        Optional<String> find = tables.stream().filter((tab) -> {
            return full_types.equals(StringUtils.substringAfter(tab, "."));
        }).findFirst();
        Assert.assertTrue("table must present:" + full_types, find.isPresent());
    }

    /**
     * Executes a JDBC statement using the default jdbc config without autocommitting the
     * connection.
     */
    protected static void initializeOracleTable(String sqlFile) {
        final String ddlFile = String.format("ddl/%s.sql", sqlFile);
        final URL ddlTestFile = TestChunjunOracleSourceFactory.class.getClassLoader().getResource(ddlFile);
        assertNotNull("Cannot locate " + ddlFile, ddlTestFile);

        oracleDS.visitAllConnection((connection) -> {
            try (InputStream reader = ddlTestFile.openStream()) {

                try (Statement statement = connection.createStatement()) {

                    final List<String> statements
                            = Arrays.stream(IOUtils.readLines(reader, TisUTF8.get()).stream().map(String::trim)
                            .filter(x -> !x.startsWith("--") && !x.isEmpty())
                            .map(
                                    x -> {
                                        final Matcher m =
                                                COMMENT_PATTERN.matcher(x);
                                        return m.matches() ? m.group(1) : x;
                                    })
                            .collect(Collectors.joining("\n"))
                            .split(";"))
                            .collect(Collectors.toList());
                    for (String stmt : statements) {
                        statement.execute(stmt);
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

        });
//        try (Connection connection = getJdbcConnection();
//             Statement statement = connection.createStatement()) {
//
//
////            final List<String> statements =
////                    Arrays.stream(
////                            Files.readAllLines(Paths.get(ddlTestFile.toURI())).stream()
////                                    .map(String::trim)
////                                    .filter(x -> !x.startsWith("--") && !x.isEmpty())
////                                    .map(
////                                            x -> {
////                                                final Matcher m =
////                                                        COMMENT_PATTERN.matcher(x);
////                                                return m.matches() ? m.group(1) : x;
////                                            })
////                                    .collect(Collectors.joining("\n"))
////                                    .split(";"))
////                            .collect(Collectors.toList());
////            for (String stmt : statements) {
////                statement.execute(stmt);
////            }
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
    }

    @Test
    public void testRecordPull() throws Exception {


        CDCTestSuitParams params //
                = CDCTestSuitParams.chunjunBuilder()

                .setIncrColumn(key_timestamp6_c)
                .setTabName(tabNameFull_types)
                .build();

        CUDCDCTestSuit cdcTestSuit = new CUDCDCTestSuit(params) {
            @Override
            protected BasicDataSourceFactory createDataSourceFactory(TargetResName dataxName) {
                return oracleDS;
            }

//            @Override
//            protected TestRow.ValProcessor getExpectValProcessor() {
//                return (rowVals, key, val) -> {
//                    if ("time_c".equals(key)) {
//                        return ((java.sql.Timestamp) val).getTime();
//                    }
//                    if ("date_c".equals(key)) {
//                        return ((java.sql.Timestamp) val).getTime();
//
//                        //  return localDateTimeToDate((LocalDateTime) val).getTime();
//                    }
//                    if ("timestamp3_c".equals(key) || "timestamp6_c".equals(key)) {
//                        return ((Timestamp) val).toLocalDateTime();
//                    }
//                    if (key_bytea_c.equals(key)) {
//                        return new String((byte[]) val);
//                    } else {
//                        return val;
//                    }
//                    // return val;
//                };
//            }

            @Override
            protected String getColEscape() {
                return "\"";
            }

//            @Override
//            protected TestRow.ValProcessor getActualValProcessor(String tabName, IResultRows consumerHandle) {
//                return (rowVals, key, val) -> {
//
//                    if ("date_c".equals(key)) {
//                        //  System.out.println(val);
//
//                        return DateTimeConverter.localDateTimeToDate((LocalDateTime) val).getTime();
//                        // ().toInstant(ZoneId.systemDefault());
//
//                    }
//
//                    if ("time_c".equals(key)) {
//                        return DateTimeConverter.localDateTimeToDate((LocalDateTime) val).getTime();
//                    }
//                    if ("timestamp3_c".equals(key) || "timestamp6_c".equals(key)) {
//                        return ((LocalDateTime) val);
//                    }
////
////                    if (val instanceof BigDecimal) {
////                        return ((BigDecimal) val).setScale(BIG_DECIMAL_SCALA);
////                    }
//                    try {
//                        if (key_bytea_c.equals(key)) {
//                            byte[] buffer = (byte[]) val;
//                            // buffer.reset();
//                            return new String(buffer);
//                        }
////                        else {
////                            return consumerHandle.deColFormat(tabName, key, val);
////                        }
//                    } catch (Exception e) {
//                        throw new RuntimeException("colKey:" + key + ",val:" + val, e);
//                    }
//
//                    return consumerHandle.deColFormat(tabName, key, val);
//                };
//            }


            @Override
            protected List<TestRow> createExampleTestRows() throws Exception {
                List<TestRow> exampleRows = Lists.newArrayList();
                Date now = new Date();
                TestRow row = null;
                Map<String, RowValsExample.RowVal> vals = null;
                int insertCount = 1;

//                CREATE TABLE full_types (
//                        id INTEGER NOT NULL,
//                        bytea_c BYTEA,
//                        small_c SMALLINT,
//                        int_c INTEGER,
//                        big_c BIGINT,
//                        real_c REAL,
//                        double_precision DOUBLE PRECISION,
//                        numeric_c NUMERIC(10, 5),
//                        decimal_c DECIMAL(10, 1),
//                        boolean_c BOOLEAN,
//                        text_c TEXT,
//                        char_c CHAR,
//                        character_c CHARACTER(3),
//                        character_varying_c CHARACTER VARYING(20),
//                        timestamp3_c TIMESTAMP(3),
//                        timestamp6_c TIMESTAMP(6),
//                        date_c DATE,
//                        time_c TIME(0),
//                        default_numeric_c NUMERIC,
//                        PRIMARY KEY (id)
//                );
//
//                ALTER TABLE full_types REPLICA IDENTITY FULL;
//
//                INSERT INTO full_types VALUES (
//                        1, '2', 32767, 65535, 2147483647, 5.5, 6.6, 123.12345, 404.4443, true,
//                        'Hello World', 'a', 'abc', 'abcd..xyz',  '2020-07-17 18:00:22.123', '2020-07-17 18:00:22.123456',
//                        '2020-07-17', '18:00:22', 500);

                for (int i = 1; i <= insertCount; i++) {
                    vals = Maps.newHashMap();
                    vals.put(tabNameFull_types_pk, RowValsExample.RowVal.$((long) i));
                    vals.put(key_bytea_c, RowValsExample.RowVal.stream("bytea_c_val"));
                    vals.put("small_c", RowValsExample.RowVal.$(2l));
                    vals.put("int_c", RowValsExample.RowVal.$(32768l));
                    vals.put("big_c", RowValsExample.RowVal.$(2147483648l));
                    vals.put("real_c", RowValsExample.RowVal.$(5.5f));
                    vals.put("double_precision", RowValsExample.RowVal.$(6.6f));
                    vals.put("numeric_c", RowValsExample.RowVal.decimal(12312345, BIG_DECIMAL_SCALA));
                    vals.put("decimal_c", RowValsExample.RowVal.decimal(4044443, BIG_DECIMAL_SCALA));
                    vals.put("boolean_c", RowValsExample.RowVal.$(1));
                    vals.put("text_c", RowValsExample.RowVal.$("Hello moto"));
                    vals.put("char_c", RowValsExample.RowVal.$("b"));
                    vals.put("character_c", RowValsExample.RowVal.$("abf"));
                    vals.put("character_varying_c", RowValsExample.RowVal.$("abcd..xyzkkkkk"));
                    vals.put("timestamp3_c", parseTimestamp("2022-07-29 18:00:22"));
                    vals.put(key_timestamp6_c, parseTimestamp("2020-07-17 18:00:22"));
                    vals.put("date_c", (parseTimestamp("2020-07-17 00:00:00")));
                    vals.put("time_c", parseTimestamp("1970-01-01 18:00:22"));
                    vals.put("default_numeric_c", RowValsExample.RowVal.decimal(500, 0));

                    row = new TestRow(RowKind.INSERT, new RowValsExample(vals));
                    row.idVal = i;
                    exampleRows.add(row);
                }
                return exampleRows;
            }

            @Override
            protected void verfiyTableCrudProcess(String tabName, BasicDataXRdbmsReader dataxReader
                    , ISelectedTab tab, IResultRows consumerHandle, IMQListener<JobExecutionResult> imqListener) throws Exception {
                super.verfiyTableCrudProcess(tabName, dataxReader, tab, consumerHandle, imqListener);
                // imqListener.start(dataxName, dataxReader, Collections.singletonList(tab), null);
                Thread.sleep(1000);
            }


        };


        ChunjunOracleSourceFactory oracleListener = new ChunjunOracleSourceFactory();

        cdcTestSuit.startTest(oracleListener);

    }


}
