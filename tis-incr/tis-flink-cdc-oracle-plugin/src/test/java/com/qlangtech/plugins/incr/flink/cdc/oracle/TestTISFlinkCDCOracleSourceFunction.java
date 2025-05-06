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

package com.qlangtech.plugins.incr.flink.cdc.oracle;

import com.google.common.collect.Sets;
import com.qlangtech.plugins.incr.flink.cdc.CDCTestSuitParams;
import com.qlangtech.plugins.incr.flink.cdc.CUDCDCTestSuit;
import com.qlangtech.plugins.incr.flink.cdc.ColMeta;
import com.qlangtech.plugins.incr.flink.cdc.IResultRows;
import com.qlangtech.plugins.incr.flink.cdc.TestRow;
import com.qlangtech.plugins.incr.flink.cdc.oracle.utils.OracleTestUtils;
import com.qlangtech.plugins.incr.flink.cdc.source.TestTableRegisterFlinkSourceHandle;
import com.qlangtech.plugins.incr.flink.junit.TISApplySkipFlinkClassloaderFactoryCreation;
import com.qlangtech.plugins.incr.flink.launch.TISFlinkCDCStreamFactory;
import com.qlangtech.plugins.incr.flink.slf4j.TISLoggerConsumer;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.async.message.client.consumer.impl.MQListenerFactory;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.datax.StoreResourceTypeConstants;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.Descriptor.FormData;
import com.qlangtech.tis.manage.common.CenterResource;
import com.qlangtech.tis.plugin.datax.common.BasicDataXRdbmsReader;
import com.qlangtech.tis.plugin.ds.BasicDataSourceFactory;
import com.qlangtech.tis.plugin.ds.CMeta;
import com.qlangtech.tis.plugin.ds.DSKey;
import com.qlangtech.tis.plugin.ds.DataSourceFactory;
import com.qlangtech.tis.plugin.ds.DataSourceFactory.IConnProcessor;
import com.qlangtech.tis.plugin.ds.DataSourceFactoryPluginStore;
import com.qlangtech.tis.plugin.ds.DataSourceMeta;
import com.qlangtech.tis.plugin.ds.JDBCConnection;
import com.qlangtech.tis.plugin.incr.TISSinkFactory;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.AbstractTestBase;
import org.junit.*;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.lifecycle.Startables;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-10-11 11:24
 * @see AbstractTestBase
 **/
public class TestTISFlinkCDCOracleSourceFunction extends OracleSourceTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(TestTISFlinkCDCOracleSourceFunction.class);

    //    private OracleContainer oracleContainer =
//            OracleTestUtils.ORACLE_CONTAINER.withLogConsumer(new TISLoggerConsumer(LOG));
    @ClassRule(order = 100)
    public static TestRule name = new TISApplySkipFlinkClassloaderFactoryCreation();

    @Before
    public void setUp() throws Exception {
        CenterResource.setNotFetchFromCenterRepository();
//        LOG.info("Starting containers...");
//        Startables.deepStart(Stream.of(oracleContainer)).join();
//        LOG.info("Containers are started.");
    }

//    @After
//    public void teardown() {
//        oracleContainer.stop();
//    }

    @Test
    public void testBinlogConsume() throws Exception {
        createAndInitialize("customer.sql");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(200L);
        env.setParallelism(1);
        TISFlinkCDCStreamFactory streamFactory = new TISFlinkCDCStreamFactory();
        streamFactory.parallelism = 1;
        FlinkCDCOracleSourceFactory oracleCDCFactory = new FlinkCDCOracleSourceFactory();
        oracleCDCFactory.startupOptions = "latest";
       // oracleCDCFactory.timeZone = MQListenerFactory.dftZoneId();
        oracleCDCFactory.independentBinLogMonitor = false;
        // debezium
        // final String tabName = "base";
        final String tabName = "base_without_lob";


        CDCTestSuitParams suitParams = CDCTestSuitParams.createBuilder().setTabName(tabName).build();


        CUDCDCTestSuit cdcTestSuit = new CUDCDCTestSuit(suitParams) {
            @Override
            protected BasicDataSourceFactory createDataSourceFactory(TargetResName dataxName, boolean useSplitTabStrategy) {
                BasicDataSourceFactory dataSourceFactory = createMySqlDataSourceFactory(dataxName);
                TIS.dsFactoryPluginStoreGetter = (p) -> {
                    DSKey key = new DSKey(StoreResourceTypeConstants.DB_GROUP_NAME, p, DataSourceFactory.class);
                    return new DataSourceFactoryPluginStore(key, false) {
                        @Override
                        public DataSourceFactory getPlugin() {
                            return dataSourceFactory;
                        }
                    };
                };
                return dataSourceFactory;
            }

            @Override
            protected void visitConnection(final IConnProcessor connProcessor) {

                // 因为Oracle创建数据的用户和 监听增量的用户是两个用户 这里是使用 TEST_USER
                try (JDBCConnection jdbcConnection = new JDBCConnection(getJdbcConnection(), ORACLE_CONTAINER.getJdbcUrl())) {
                    connProcessor.vist(jdbcConnection);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

                //  Objects.requireNonNull(dataSourceFactory, "dataSourceFactory can not be null").visitFirstConnection(connProcessor);
            }

            @Override
            protected void insertTestRow(JDBCConnection conn, TestRow r) throws SQLException {
                super.insertTestRow(conn, r);
            }

            @Override
            protected EntityName createTableName(String tabName) {
                return EntityName.parse(ORACLE_SCHEMA + "." + tabName, true, false);
            }

            @Override
            protected IResultRows createConsumerHandle(BasicDataXRdbmsReader dataxReader, String tabName, TISSinkFactory sinkFuncFactory) {
                TestTableRegisterFlinkSourceHandle sourceHandle = new TestTableRegisterFlinkSourceHandle(tabName, cols);
                sourceHandle.setSinkFuncFactory(sinkFuncFactory);
                sourceHandle.setSourceStreamTableMeta(dataxReader);
                sourceHandle.setStreamFactory(streamFactory);
                sourceHandle.setSourceFlinkColCreator(oracleCDCFactory.createFlinkColCreator(dataxReader));
                return sourceHandle;
            }

            @Override
            protected void startProcessConn(JDBCConnection conn) throws SQLException {
                conn.getConnection().setAutoCommit(false);
            }

            @Override
            protected int executePreparedStatement(Connection connection, PreparedStatement statement) throws SQLException {
                int count = super.executePreparedStatement(connection, statement);
                connection.commit();
                return count;
            }

            @Override
            protected int executeStatement(Connection connection, Statement statement, String sql) throws SQLException {
                int count = super.executeStatement(connection, statement, sql);
                connection.commit();
                return count;
            }

            @Override
            protected void validateTableName() {
                return;
            }

            private final Set<String> clobTypeFields = Sets.newHashSet(key_json_content, keyColBlob, keyCol_text);

            protected List<ColMeta> getAssertCols() {
                // 由于oracle对于clob类型的字段监听有问题，需要先将clob类型的字段去掉
                return cols.stream().filter((col) -> !clobTypeFields.contains(col.getName())).collect(Collectors.toUnmodifiableList());
            }

            protected String getEscapedCol(CMeta col) {
                return col.getName();
            }
        };

        cdcTestSuit.startTest(oracleCDCFactory);

    }


    protected BasicDataSourceFactory createMySqlDataSourceFactory(TargetResName dataxName) {
        Descriptor mySqlV5DataSourceFactory = TIS.get().getDescriptor("OracleDataSourceFactory");
        Assert.assertNotNull(mySqlV5DataSourceFactory);

        Descriptor.FormData formData = new Descriptor.FormData();
        formData.addProp("name", "oracle");
        formData.addProp("dbName", ORACLE_CONTAINER.getSid());
        formData.addProp("nodeDesc", ORACLE_CONTAINER.getHost());
//        formData.addProp("password", OracleTestUtils.ORACLE_PWD);
//        formData.addProp("userName", OracleTestUtils.ORACLE_USER);
        formData.addProp("password", ORACLE_CONTAINER.getPassword());
        formData.addProp("userName", ORACLE_CONTAINER.getUsername());


//        formData.addProp("password", TEST_PWD);
//        formData.addProp("userName", TEST_USER);


        formData.addProp("port", String.valueOf(ORACLE_CONTAINER.getOraclePort()));

        formData.addProp("asServiceName", "false");
        FormData authorized = new FormData();
        authorized.addProp("schema", ORACLE_SCHEMA);
        formData.addSubForm("allAuthorized", "com.qlangtech.tis.plugin.ds.oracle.auth.AcceptAuthorized", authorized);

        FormData connEntity = new FormData();
        connEntity.addProp("serviceName", ORACLE_CONTAINER.getDatabaseName());
        formData.addSubForm("connEntity", "com.qlangtech.tis.plugin.ds.oracle.impl.ServiceNameConnEntity", connEntity);

        Descriptor.ParseDescribable<BasicDataSourceFactory> parseDescribable
                = mySqlV5DataSourceFactory.newInstance(dataxName.getName(), formData);
        Assert.assertNotNull(parseDescribable.getInstance());

        return parseDescribable.getInstance();
    }
}
