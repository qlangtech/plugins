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

package com.qlangtech.plugins.incr.flink.cdc.mongdb;

import com.google.common.collect.Maps;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.qlangtech.plugins.incr.flink.cdc.CDCTestSuitParams;
import com.qlangtech.plugins.incr.flink.cdc.CUDCDCTestSuit;
import com.qlangtech.plugins.incr.flink.cdc.ColMeta;
import com.qlangtech.plugins.incr.flink.cdc.IResultRows;
import com.qlangtech.plugins.incr.flink.cdc.RowValsExample.RowVal;
import com.qlangtech.plugins.incr.flink.cdc.RowValsUpdate;
import com.qlangtech.plugins.incr.flink.cdc.TestRow;
import com.qlangtech.plugins.incr.flink.cdc.mongdb.impl.startup.LatestOffset;
import com.qlangtech.plugins.incr.flink.cdc.mongdb.impl.updatecomplete.UpdateLookup;
import com.qlangtech.plugins.incr.flink.cdc.mongdb.utils.MongoDBTestUtils;
import com.qlangtech.plugins.incr.flink.cdc.source.TestTableRegisterFlinkSourceHandle;
import com.qlangtech.plugins.incr.flink.junit.TISApplySkipFlinkClassloaderFactoryCreation;
import com.qlangtech.plugins.incr.flink.launch.TISFlinkCDCStreamFactory;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.async.message.client.consumer.impl.MQListenerFactory;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.plugin.datax.DataXMongodbReader;
import com.qlangtech.tis.plugin.datax.SelectedTab;
import com.qlangtech.tis.plugin.datax.common.BasicDataXRdbmsReader;
import com.qlangtech.tis.plugin.datax.common.RdbmsReaderContext;
import com.qlangtech.tis.plugin.datax.mongo.MongoCMeta;
import com.qlangtech.tis.plugin.ds.CMeta;
import com.qlangtech.tis.plugin.ds.DSKey;
import com.qlangtech.tis.plugin.ds.DataSourceFactory;
import com.qlangtech.tis.plugin.ds.DataSourceFactory.IConnProcessor;
import com.qlangtech.tis.plugin.ds.DataSourceFactoryPluginStore;
import com.qlangtech.tis.plugin.ds.IDataSourceDumper;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.ds.JDBCConnection;
import com.qlangtech.tis.plugin.ds.mangodb.MangoDBDataSourceFactory;
import com.qlangtech.tis.plugin.ds.mangodb.MangoDBDataSourceFactory.MongoJDBCConnection;
import com.qlangtech.tis.plugin.incr.TISSinkFactory;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.bson.Document;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-12-05 08:33
 **/
@RunWith(Parameterized.class)
public class TestTISFlinkCDCMongoDBSourceFunction extends MongoDBSourceTestBase {
    //    @Rule
//    public final Timeout timeoutPerTest = Timeout.seconds(300);
    @ClassRule(order = 100)
    public static TestRule name = new TISApplySkipFlinkClassloaderFactoryCreation();
    private final String mongoVersion;
    private final boolean parallelismSnapshot;

    public TestTISFlinkCDCMongoDBSourceFunction(String mongoVersion, boolean parallelismSnapshot) {
        super(mongoVersion);
        this.mongoVersion = mongoVersion;
        this.parallelismSnapshot = parallelismSnapshot;
    }

    @Parameterized.Parameters(name = "mongoVersion: {0} parallelismSnapshot: {1}")
    public static Object[] parameters() {
        List<Object[]> parameterTuples = new ArrayList<>();
        for (String mongoVersion : getMongoVersions()) {
            parameterTuples.add(new Object[]{mongoVersion, true});
            //   parameterTuples.add(new Object[]{mongoVersion, false});
        }
        return parameterTuples.toArray();
    }

    static final String tabNameBase = "base";

    @Test
    public void testBinlogConsume() throws Exception {

        final String createdDB = testMongoDBParallelSource(
                1,
                MongoDBTestUtils.FailoverType.NONE,
                MongoDBTestUtils.FailoverPhase.NEVER,
                new String[]{tabNameBase});


//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.enableCheckpointing(200L);
//        env.setParallelism(1);
        TISFlinkCDCStreamFactory streamFactory = new TISFlinkCDCStreamFactory();
        streamFactory.parallelism = 1;
        FlinkCDCMongoDBSourceFactory mongoDBCDCFactory = new FlinkCDCMongoDBSourceFactory();
        mongoDBCDCFactory.startupOption = new LatestOffset();
        //mongoDBCDCFactory.timeZone = MQListenerFactory.dftZoneId();
        //  mongoDBCDCFactory.updateRecordComplete = new UpdateLookup();

        mongoDBCDCFactory.updateRecordComplete = new UpdateLookup();
//        mongoDBCDCFactory.updateRecordComplete = new FullChangelog();
        //  mongoDBCDCFactory.updateRecordComplete = new NoneLookup();


        CDCTestSuitParams suitParams = CDCTestSuitParams.createBuilder()
                .setTabName(tabNameBase)
                .setUpdateRowKind( //RowKind.UPDATE_BEFORE,
                        mongoDBCDCFactory.updateRecordComplete.getUpdateRowkindsForTest()).build();


        CUDCDCTestSuit cdcTestSuit = new CUDCDCTestSuit(suitParams) {

            @Override
            protected boolean isSkipAssertTest() {
                return true;
            }

            @Override
            protected Supplier<CMeta> getCmetaCreator() {
                return () -> new MongoCMeta();
            }

            @Override
            protected BasicDataXRdbmsReader createDataxReader(TargetResName dataxName, String tabName) {

                BasicDataXRdbmsReader rdbmsReader = super.createDataxReader(dataxName, tabName);
                DataXMongodbReader mongoReader = new DataXMongodbReader() {
                    @Override
                    public MangoDBDataSourceFactory getDataSourceFactory() {
                        return (MangoDBDataSourceFactory) rdbmsReader.getDataSourceFactory();
                    }
                };
                mongoReader.selectedTabs = rdbmsReader.selectedTabs;
                mongoReader.timeZone = MQListenerFactory.dftZoneId();
                return mongoReader;
//                DataSourceFactory dataSourceFactory = createDataSourceFactory(dataxName, this.splitTabSuffix.isPresent());
//                BasicDataXRdbmsReader dataxReader = new BasicDataXRdbmsReader() {
//                    @Override
//                    protected RdbmsReaderContext createDataXReaderContext(String jobName, SelectedTab tab, IDataSourceDumper dumper) {
//                        return null;
//                    }
//
//                    @Override
//                    public DataSourceFactory getDataSourceFactory() {
//                        return dataSourceFactory;
//                    }
//                };
//
//                SelectedTab baseTab = createSelectedTab(tabName, dataSourceFactory);
//                dataxReader.selectedTabs = Collections.singletonList(baseTab);
//                return dataxReader;
            }


            @Override
            protected DataSourceFactory createDataSourceFactory(TargetResName dataxName, boolean useSplitTabStrategy) {
                dataSourceFactory = createMySqlDataSourceFactory(dataxName, createdDB);
                TIS.dsFactoryPluginStoreGetter = (p) -> {
                    DSKey key = new DSKey(TIS.DB_GROUP_NAME, p, DataSourceFactory.class);
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
            protected void startProcessConn(JDBCConnection conn) throws SQLException {
                //conn.getConnection().setAutoCommit(false);
            }

            @Override
            protected void visitConnection(final IConnProcessor connProcessor) {


                Objects.requireNonNull(dataSourceFactory, "dataSourceFactory can not be null").visitFirstConnection(connProcessor);
            }

            @Override
            protected Map<String, RowVal> createInsertRowValMap(int insertIndex) {
                Map<String, RowVal> vals = Maps.newHashMap();
                vals.put("_id", RowVal.$("5d505646cf6d4fe581014ab" + insertIndex));
                return vals;
            }

            @Override
            protected void insertTestRow(JDBCConnection conn, TestRow r) throws SQLException {
                //  super.insertTestRow(conn, r);
                MongoJDBCConnection mongoConn = (MongoJDBCConnection) conn;

                MongoClient mongoClient = mongoConn.getMongoClient();
                List<ColMeta> cols = this.getAssertCols();
                MongoStatementSetter statementSetter = new MongoStatementSetter();
                for (ColMeta col : cols) {
                    col.setTestVal(statementSetter, r);
                }
                mongoClient.getDatabase(createdDB)
                        .getCollection(tabNameBase).insertOne(statementSetter.createDoc());
            }

            @Override
            protected void updateTestRow(ISelectedTab tab, TestRow testRow, JDBCConnection connection) {
                UpdateResult updateResult = null;
                try {
                    MongoJDBCConnection mongoConn = (MongoJDBCConnection) connection;
                    List<Map.Entry<String, RowValsUpdate.UpdatedColVal>> cols = testRow.getUpdateValsCols();
                    MongoClient mongoClient = mongoConn.getMongoClient();

                    MongoStatementSetter statementSetter = new MongoStatementSetter();

                    for (Map.Entry<String, RowValsUpdate.UpdatedColVal> col : cols) {
                        col.getValue().setPrepColVal(statementSetter, testRow.vals);
                    }


                    Document filter = new Document(keyBaseId, testRow.getIdVal());
                    updateResult = mongoClient.getDatabase(createdDB)
                            .getCollection(tabNameBase)
                            .updateOne(filter, new Document("$set", statementSetter.createDoc()));
                    //  System.out.println(updateResult);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

                Assert.assertNotNull("updateResult can not be null", updateResult);
                Assert.assertEquals("matchedCount must be 1", 1, updateResult.getMatchedCount());
            }

            @Override
            protected void deleteTestRow(ISelectedTab tab, TestRow testRow, JDBCConnection connection) throws SQLException {
                DeleteResult deleteResult = null;
                try {
                    MongoJDBCConnection mongoConn = (MongoJDBCConnection) connection;
                    // List<Map.Entry<String, RowValsUpdate.UpdatedColVal>> cols = testRow.getUpdateValsCols();
                    MongoClient mongoClient = mongoConn.getMongoClient();

                    // MongoStatementSetter statementSetter = new MongoStatementSetter();

//                    for (Map.Entry<String, RowValsUpdate.UpdatedColVal> col : cols) {
//                        col.getValue().setPrepColVal(statementSetter, testRow.vals);
//                    }
                    Document filter = new Document(keyBaseId, testRow.getIdVal());
                    deleteResult = mongoClient.getDatabase(createdDB)
                            .getCollection(tabNameBase)
                            .deleteOne(filter);
                    //  System.out.println(updateResult);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

                Assert.assertNotNull("updateResult can not be null", deleteResult);
                Assert.assertEquals("delete count must be 1", 1, deleteResult.getDeletedCount());
            }

            @Override
            protected EntityName createTableName(String tabName) {
                //  return EntityName.parse(ORACLE_SCHEMA + "." + tabName, true, false);
                throw new UnsupportedOperationException();
            }

            @Override
            protected IResultRows createConsumerHandle(BasicDataXRdbmsReader dataxReader, String tabName, TISSinkFactory sinkFuncFactory) {
                TestTableRegisterFlinkSourceHandle sourceHandle = new TestTableRegisterFlinkSourceHandle(tabName, cols);
                sourceHandle.setSinkFuncFactory(sinkFuncFactory);
                sourceHandle.setSourceStreamTableMeta(dataxReader);
                sourceHandle.setStreamFactory(streamFactory);
                sourceHandle.setSourceFlinkColCreator(mongoDBCDCFactory.createFlinkColCreator());
                return sourceHandle;
            }


            @Override
            protected void validateTableName() {
                return;
            }

//            private final Set<String> clobTypeFields = Sets.newHashSet(key_json_content, keyColBlob, keyCol_text);
//
//            protected List<ColMeta> getAssertCols() {
//                // 由于oracle对于clob类型的字段监听有问题，需要先将clob类型的字段去掉
//                return cols.stream().filter((col) -> !clobTypeFields.contains(col.getName())).collect(Collectors.toUnmodifiableList());
//            }

            protected String getEscapedCol(CMeta col) {
                return col.getName();
            }
        };

        cdcTestSuit.startTest(mongoDBCDCFactory);

        Thread.sleep(999999999);
    }

    protected DataSourceFactory createMySqlDataSourceFactory(TargetResName dataxName, String createdDB) {
        MangoDBDataSourceFactory mongoSourceFactory = new MangoDBDataSourceFactory() {
//            @Override
//            public List<ColumnMetaData> getTableMetadata(boolean inSink, EntityName table) {
//                return vistMongoClient((client) -> {
//                    List<ColumnMetaData> cols = Lists.newArrayList();
//                    //  int index, String key, DataType type, boolean pk, boolean nullable
//                    cols.add(new ColumnMetaData(0, keyBaseId, DataType.getType(JDBCTypes.BIGINT), true, false));
//                    cols.add(new ColumnMetaData(1, keyStart_time, DataType.getType(JDBCTypes.TIMESTAMP), false, false));
//                    cols.add(new ColumnMetaData(2, key_update_date, DataType.getType(JDBCTypes.DATE), false, false));
//                    cols.add(new ColumnMetaData(3, key_update_time, DataType.getType(JDBCTypes.TIMESTAMP), false, false));
//                    cols.add(new ColumnMetaData(4, key_price, DataType.getType(JDBCTypes.DECIMAL), false, false));
//                    cols.add(new ColumnMetaData(5, key_json_content, DataType.getType(JDBCTypes.LONGVARCHAR), false, false));
//                    cols.add(new ColumnMetaData(6, keyColBlob, DataType.getType(JDBCTypes.BLOB), false, false));
//                    cols.add(new ColumnMetaData(7, keyCol_text, DataType.getType(JDBCTypes.LONGVARCHAR), false, false));
//
//                    return cols;
//                });
//            }
        };
        mongoSourceFactory.name = "mongodb";
        mongoSourceFactory.dbName = createdDB;
//        mongoSourceFactory.username = ;
//        mongoSourceFactory.password = ;
        mongoSourceFactory.address = mongoContainer.getHostAndPort();
        mongoSourceFactory.userSource = "admin";
        mongoSourceFactory.authMechanism = MangoDBDataSourceFactory.dftAuthMechanism();
        mongoSourceFactory.inspectRowCount = 500;
        System.out.println(mongoSourceFactory.toString());
        return mongoSourceFactory;
//        Descriptor mySqlV5DataSourceFactory = TIS.get().getDescriptor("MangoDBDataSourceFactory");
//        Assert.assertNotNull(mySqlV5DataSourceFactory);
//
//        Descriptor.FormData formData = new Descriptor.FormData();
//        formData.addProp("name", "mongodb");
//        formData.addProp("dbName", createdDB);
//
//        formData.addProp("address", mongoContainer.getHostAndPort());
//        formData.addProp("username", null);
//        formData.addProp("password", null);
//        formData.addProp("userSource", "admin");
//        formData.addProp("authMechanism", "usernamePasswordAuthMethod");
//
//
//        Descriptor.ParseDescribable<BasicDataSourceFactory> parseDescribable
//                = mySqlV5DataSourceFactory.newInstance(dataxName.getName(), formData);
//        Assert.assertNotNull(parseDescribable.getInstance());
//
//        return parseDescribable.getInstance();
    }


    private void testMongoDBParallelSource(
            MongoDBTestUtils.FailoverType failoverType,
            MongoDBTestUtils.FailoverPhase failoverPhase,
            String[] captureCustomerCollections)
            throws Exception {
        testMongoDBParallelSource(
                DEFAULT_PARALLELISM, failoverType, failoverPhase, captureCustomerCollections);
    }

    private String testMongoDBParallelSource(
            int parallelism,
            MongoDBTestUtils.FailoverType failoverType,
            MongoDBTestUtils.FailoverPhase failoverPhase,
            String[] captureCustomerCollections)
            throws Exception {
        return testMongoDBParallelSource(
                parallelism, failoverType, failoverPhase, captureCustomerCollections, false);
    }

    private String testMongoDBParallelSource(
            int parallelism,
            MongoDBTestUtils.FailoverType failoverType,
            MongoDBTestUtils.FailoverPhase failoverPhase,
            String[] captureCustomerCollections,
            boolean skipSnapshotBackfill)
            throws Exception {

        String customerDatabase =
                "customer_" + Integer.toUnsignedString(new Random().nextInt(), 36);

        // A - enable system-level fulldoc pre & post image feature
        mongoContainer.executeCommand(
                "use admin; db.runCommand({ setClusterParameter: { changeStreamOptions: { preAndPostImages: { expireAfterSeconds: 'off' } } } })");

        // B - enable collection-level fulldoc pre & post image for change capture collection
        for (String collectionName : captureCustomerCollections) {
            mongoContainer.executeCommandInDatabase(
                    String.format(
                            "db.createCollection('%s'); db.runCommand({ collMod: '%s', changeStreamPreAndPostImages: { enabled: true } })",
                            collectionName, collectionName),
                    customerDatabase);
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        env.setParallelism(parallelism);
        env.enableCheckpointing(200L);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));
        mongoContainer.executeCommandFileInDatabase(tabNameBase, customerDatabase);
        return customerDatabase;

//        String sourceDDL =
//                String.format(
//                        "CREATE TABLE customers ("
//                                + " _id STRING NOT NULL,"
//                                + " cid BIGINT NOT NULL,"
//                                + " name STRING,"
//                                + " address STRING,"
//                                + " phone_number STRING,"
//                                + " primary key (_id) not enforced"
//                                + ") WITH ("
//                                + " 'connector' = 'mongodb-cdc',"
//                                + " 'scan.incremental.snapshot.enabled' = '%s',"
//                                + " 'hosts' = '%s',"
//                                + " 'username' = '%s',"
//                                + " 'password' = '%s',"
//                                + " 'database' = '%s',"
//                                + " 'collection' = '%s',"
//                                + " 'heartbeat.interval.ms' = '500',"
//                                + " 'scan.full-changelog' = 'true',"
//                                + " 'scan.incremental.snapshot.backfill.skip' = '%s'"
//                                + ")",
//                        parallelismSnapshot ? "true" : "false",
//                        mongoContainer.getHostAndPort(),
//                        FLINK_USER,
//                        FLINK_USER_PASSWORD,
//                        customerDatabase,
//                        getCollectionNameRegex(customerDatabase, captureCustomerCollections),
//                        skipSnapshotBackfill);
//
//        mongoContainer.executeCommandFileInDatabase("customer", customerDatabase);
//
//        // first step: check the snapshot data
//        String[] snapshotForSingleTable =
//                new String[]{
//                        "+I[101, user_1, Shanghai, 123567891234]",
//                        "+I[102, user_2, Shanghai, 123567891234]",
//                        "+I[103, user_3, Shanghai, 123567891234]",
//                        "+I[109, user_4, Shanghai, 123567891234]",
//                        "+I[110, user_5, Shanghai, 123567891234]",
//                        "+I[111, user_6, Shanghai, 123567891234]",
//                        "+I[118, user_7, Shanghai, 123567891234]",
//                        "+I[121, user_8, Shanghai, 123567891234]",
//                        "+I[123, user_9, Shanghai, 123567891234]",
//                        "+I[1009, user_10, Shanghai, 123567891234]",
//                        "+I[1010, user_11, Shanghai, 123567891234]",
//                        "+I[1011, user_12, Shanghai, 123567891234]",
//                        "+I[1012, user_13, Shanghai, 123567891234]",
//                        "+I[1013, user_14, Shanghai, 123567891234]",
//                        "+I[1014, user_15, Shanghai, 123567891234]",
//                        "+I[1015, user_16, Shanghai, 123567891234]",
//                        "+I[1016, user_17, Shanghai, 123567891234]",
//                        "+I[1017, user_18, Shanghai, 123567891234]",
//                        "+I[1018, user_19, Shanghai, 123567891234]",
//                        "+I[1019, user_20, Shanghai, 123567891234]",
//                        "+I[2000, user_21, Shanghai, 123567891234]"
//                };
//        tEnv.executeSql(sourceDDL);
//        TableResult tableResult =
//                tEnv.executeSql("select cid, name, address, phone_number from customers");
//        CloseableIterator<Row> iterator = tableResult.collect();
//        JobID jobId = tableResult.getJobClient().get().getJobID();
//        List<String> expectedSnapshotData = new ArrayList<>();
//        for (int i = 0; i < captureCustomerCollections.length; i++) {
//            expectedSnapshotData.addAll(Arrays.asList(snapshotForSingleTable));
//        }
//
//        // trigger failover after some snapshot splits read finished
//        if (failoverPhase == MongoDBTestUtils.FailoverPhase.SNAPSHOT && iterator.hasNext()) {
//            triggerFailover(
//                    failoverType, jobId, miniClusterResource.getMiniCluster(), () -> sleepMs(100));
//        }
//
//        assertEqualsInAnyOrder(
//                expectedSnapshotData, fetchRows(iterator, expectedSnapshotData.size()));
//
//        // second step: check the change stream data
//        for (String collectionName : captureCustomerCollections) {
//            makeFirstPartChangeStreamEvents(
//                    mongodbClient.getDatabase(customerDatabase), collectionName);
//        }
//        if (failoverPhase == MongoDBTestUtils.FailoverPhase.STREAM) {
//            triggerFailover(
//                    failoverType, jobId, miniClusterResource.getMiniCluster(), () -> sleepMs(200));
//        }
//        for (String collectionName : captureCustomerCollections) {
//            makeSecondPartChangeStreamEvents(
//                    mongodbClient.getDatabase(customerDatabase), collectionName);
//        }
//
//        String[] changeEventsForSingleTable =
//                new String[]{
//                        "-U[101, user_1, Shanghai, 123567891234]",
//                        "+U[101, user_1, Hangzhou, 123567891234]",
//                        "-D[102, user_2, Shanghai, 123567891234]",
//                        "+I[102, user_2, Shanghai, 123567891234]",
//                        "-U[103, user_3, Shanghai, 123567891234]",
//                        "+U[103, user_3, Hangzhou, 123567891234]",
//                        "-U[1010, user_11, Shanghai, 123567891234]",
//                        "+U[1010, user_11, Hangzhou, 123567891234]",
//                        "+I[2001, user_22, Shanghai, 123567891234]",
//                        "+I[2002, user_23, Shanghai, 123567891234]",
//                        "+I[2003, user_24, Shanghai, 123567891234]"
//                };
//        List<String> expectedChangeStreamData = new ArrayList<>();
//        for (int i = 0; i < captureCustomerCollections.length; i++) {
//            expectedChangeStreamData.addAll(Arrays.asList(changeEventsForSingleTable));
//        }
//        List<String> actualChangeStreamData = fetchRows(iterator, expectedChangeStreamData.size());
//        assertEqualsInAnyOrder(expectedChangeStreamData, actualChangeStreamData);
//        tableResult.getJobClient().get().cancel().get();
    }

    private void makeSecondPartChangeStreamEvents(MongoDatabase mongoDatabase, String collection) {
        MongoCollection<Document> mongoCollection = mongoDatabase.getCollection(collection);
        mongoCollection.updateOne(Filters.eq("cid", 1010L), Updates.set("address", "Hangzhou"));
        mongoCollection.insertMany(
                Arrays.asList(
                        customerDocOf(2001L, "user_22", "Shanghai", "123567891234"),
                        customerDocOf(2002L, "user_23", "Shanghai", "123567891234"),
                        customerDocOf(2003L, "user_24", "Shanghai", "123567891234")));
    }

    private Document customerDocOf(Long cid, String name, String address, String phoneNumber) {
        Document document = new Document();
        document.put("cid", cid);
        document.put("name", name);
        document.put("address", address);
        document.put("phone_number", phoneNumber);
        return document;
    }

    private String getCollectionNameRegex(String database, String[] captureCustomerCollections) {
        checkState(captureCustomerCollections.length > 0);
        if (captureCustomerCollections.length == 1) {
            return database + "." + captureCustomerCollections[0];
        } else {
            // pattern that matches multiple collections
            return Arrays.stream(captureCustomerCollections)
                    .map(coll -> "^(" + database + "." + coll + ")$")
                    .collect(Collectors.joining("|"));
        }
    }

    private void makeFirstPartChangeStreamEvents(MongoDatabase mongoDatabase, String collection) {
        MongoCollection<Document> mongoCollection = mongoDatabase.getCollection(collection);
        mongoCollection.updateOne(Filters.eq("cid", 101L), Updates.set("address", "Hangzhou"));
        mongoCollection.deleteOne(Filters.eq("cid", 102L));
        mongoCollection.insertOne(customerDocOf(102L, "user_2", "Shanghai", "123567891234"));
        mongoCollection.updateOne(Filters.eq("cid", 103L), Updates.set("address", "Hangzhou"));
    }

    private void sleepMs(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ignored) {
        }
    }

}
