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

import com.dtstack.chunjun.connector.doris.rest.DorisStreamLoad;
import com.dtstack.chunjun.connector.doris.rest.FeRestService;
import com.dtstack.chunjun.connector.doris.rest.module.BackendRow;
import com.qlangtech.tis.manage.common.Config;
import com.qlangtech.tis.plugin.datax.BasicDorisStarRocksWriter;
import com.qlangtech.tis.plugin.datax.common.BasicDataXRdbmsWriter;
import com.qlangtech.tis.plugin.datax.doris.DataXDorisWriter;
import com.qlangtech.tis.plugin.ds.BasicDataSourceFactory;
import com.qlangtech.tis.plugin.ds.doris.DorisSourceFactory;
import com.qlangtech.tis.plugins.incr.flink.connector.mysql.ChunjunSinkFactory;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpEntity;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.ContainerState;
import org.testcontainers.containers.DockerComposeContainer;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-08-15 16:01
 **/
public class TestChunjunDorisSinkFactory extends TestFlinkSinkExecutor {

    private static final int DORIS_FE_PORT = 9030;
    private static final int DORIS_FE_LOAD_PORT = 8030;
    private static final int DORIS_BE_PORT = 9050;
    private static final int DORIS_BE_LOAD_PORT = 8040;
    private static final String DORIS_FE_SERVICE = "doris-fe_1";
    private static final String DORIS_BE_SERVICE = "doris-be_1";

    private static final Logger logger = LoggerFactory.getLogger(TestChunjunDorisSinkFactory.class);

    static {

    }


    static String feServiceHost;


    @ClassRule
    public static DockerComposeContainer environment =
            new DockerComposeContainer(new File("src/test/resources/compose-doris-test.yml"))
                    .withExposedService(DORIS_FE_SERVICE, DORIS_FE_PORT)
                    .withExposedService(DORIS_FE_SERVICE, DORIS_FE_LOAD_PORT)
                    .withExposedService(DORIS_BE_SERVICE, DORIS_BE_PORT)
                    .withExposedService(DORIS_BE_SERVICE, DORIS_BE_LOAD_PORT);
//                    .withExposedService("elasticsearch_1", ELASTICSEARCH_PORT);

    protected static DorisSourceFactory dsFactory;

    @BeforeClass
    public static void initializeDorisDB() throws Exception {


        Assert.assertNotNull(environment);

        feServiceHost = environment.getServiceHost(DORIS_FE_SERVICE, DORIS_FE_PORT);
        int jdbcPort = environment.getServicePort(DORIS_FE_SERVICE, DORIS_FE_PORT);
        int loadPort = environment.getServicePort(DORIS_FE_SERVICE, DORIS_FE_LOAD_PORT);
        System.out.println(feServiceHost + ":" + jdbcPort + "_" + loadPort);

        // 客户端会向fe请求到be的地址，然后直接向be发送数据
        FeRestService.backendRequestStub = () -> {
            BackendRow backend = new BackendRow();
            backend.setAlive(true);
            backend.setIP(environment.getServiceHost(DORIS_BE_SERVICE, DORIS_BE_LOAD_PORT));
            backend.setHttpPort(String.valueOf(environment.getServicePort(DORIS_BE_SERVICE, DORIS_BE_LOAD_PORT)));
            return backend;
        };


//        String beHost = environment.getServiceHost(DORIS_BE_SERVICE, DORIS_BE_PORT);
//        System.out.println("beHost:"+beHost);
        Optional<ContainerState> containerByServiceName = environment.getContainerByServiceName(DORIS_BE_SERVICE);
        Assert.assertTrue(containerByServiceName.isPresent());
        //  System.out.println(containerByServiceName.get().);
        final String beContainerName = containerByServiceName.get().getContainerInfo().getName();

        String colName = null;
        dsFactory = getDorisSourceFactory(feServiceHost, jdbcPort, loadPort);
        try (Connection conn = dsFactory.getConnection(
                dsFactory.buidJdbcUrl(null, feServiceHost, null))) {

            try (Statement statement = conn.createStatement()) {
//                System.out.println("beContainerName:" + beContainerName);
//                Thread.sleep(1000000);
                statement.execute("ALTER SYSTEM ADD BACKEND \"" + StringUtils.substringAfter(beContainerName, "/") + ":" + DORIS_BE_PORT + "\"");
            }
            Thread.sleep(10000);
            try (Statement statement = conn.createStatement()) {
                try (ResultSet result = statement.executeQuery("SHOW PROC '/backends'")) {
                    ResultSetMetaData metaData = result.getMetaData();
                    if (result.next()) {
                        for (int i = 1; i <= metaData.getColumnCount(); i++) {
                            colName = metaData.getColumnName(i);
                            System.out.println(colName + ":" + result.getString(colName) + " ");
                        }
                        Assert.assertTrue("be node must be alive", result.getBoolean("Alive"));

                    } else {
                        Assert.fail("must has backend node");
                    }
                }
                statement.execute("create database if not exists " + dbName);
                statement.execute( //
                        "create table if not exists " + dbName + ".test_table(\n" +
                                "       name varchar(100),\n" +
                                "       value float\n" +
                                ")\n" +
                                "ENGINE=olap\n" +
                                "UNIQUE KEY(name)\n" +
                                "DISTRIBUTED BY HASH(name)\n" +
                                "PROPERTIES(\"replication_num\" = \"1\")");
                statement.execute("insert into " + dbName + ".test_table values (\"nick\", 1), (\"nick2\", 3)");

                dsFactory.dbName = dbName;
            }
            Assert.assertNotNull(conn);
        }
    }

    protected BasicDataSourceFactory getDsFactory() {
        return dsFactory;
    }

//    private Runner httpStub;
//
//    @Before
//    public void beforeRun() throws Exception {
//
//        HttpServer server = jsonHttpServer(8080
//                , file("src/test/resources/com/qlangtech/plugins/incr/flink/chunjun/doris/sink/doris_be_host_response.json"));
//        this.httpStub = runner(server);
//        this.httpStub.start();
//    }
//
//    @After
//    public void afterRun() throws Exception {
//        httpStub.stop();
//    }


    /**
     * https://doris.apache.org/docs/data-operate/import/import-way/stream-load-manual
     * https://github.com/apache/doris/blob/1b0b5b5f0940f37811fc9bdce8d148766e46f6cb/docs/zh-CN/docs/sql-manual/sql-reference/Data-Manipulation-Statements/Load/STREAM-LOAD.md
     * <p>
     * 可以使用sequence列来更新
     * https://github.com/apache/doris/blob/d7770db5e21d9e681520da7e47320b5895e7d5f5/docs/zh-CN/docs/data-operate/update-delete/sequence-column-manual.md
     * <p>
     * 批量删除
     * https://github.com/apache/doris/blob/2d39cffa5cd19ae1984a1459532072583e9b78df/docs/zh-CN/docs/data-operate/update-delete/batch-delete-manual.md
     *
     * @throws Exception
     */
    @Test
    public void testSinkSync() throws Exception {

        AtomicInteger httpPutCount = new AtomicInteger();
        DorisStreamLoad.httpPutConsumer = (httpPut) -> {
            HttpEntity putEntity = httpPut.getEntity();
            httpPutCount.incrementAndGet();
        };

        super.testSinkSync();

        Assert.assertEquals("httpPutCount must be 1", 1, httpPutCount.get());
        //  System.out.println("logger.getClass():" + logger.getClass());
//
//        /**
//         建表需要将update_time作为uniqueKey的一部分，不然更新会有问题
//         CREATE TABLE `totalpayinfo` (
//         `id` varchar(32) NULL COMMENT "",
//         `update_time` DATETIME   NULL,
//         `entity_id` varchar(10) NULL COMMENT "",
//         `num` int(11) NULL COMMENT "",
//         `create_time` bigint(20) NULL COMMENT "",
//         `update_date` DATE       NULL,
//         `start_time`  DATETIME   NULL
//         ) ENGINE=OLAP
//         UNIQUE KEY(`id`,`update_time`)
//         DISTRIBUTED BY HASH(`id`) BUCKETS 10
//         PROPERTIES (
//         "replication_num" = "1"
//         );
//         * */
//
//        try {
//
//            String[] colNames = new String[]{colEntityId, colNum, colId, colCreateTime, updateTime, updateDate, starTime};
//
//            DataxProcessor dataxProcessor = mock("dataxProcessor", DataxProcessor.class);
//
//            File ddlDir = folder.newFolder("ddl");
//            String tabSql = tableName + IDataxProcessor.DATAX_CREATE_DDL_FILE_NAME_SUFFIX;
//            FileUtils.write(new File(ddlDir, tabSql)
//                    , IOUtils.loadResourceFromClasspath(this.getClass(), tabSql), TisUTF8.get());
//
//            EasyMock.expect(dataxProcessor.getDataxCreateDDLDir(null)).andReturn(ddlDir);
//
//            DataxProcessor.processorGetter = (name) -> {
//                return dataxProcessor;
//            };
//            IDataxReader dataxReader = mock("dataxReader", IDataxReader.class);
//            List<ISelectedTab> selectedTabs = Lists.newArrayList();
//
//
//            SinkTabPropsExtends sinkExt = new SinkTabPropsExtends();
//            sinkExt.tabName = tableName;
//
//
//            //  EasyMock.expect(sinkExt.tabName).andReturn(tableName).times(2);
//            //InsertType updateMode = new InsertType();
//
//            ReplaceType updateMode = new ReplaceType();
//            updateMode.updateKey = Lists.newArrayList(colId, updateTime);
//            // EasyMock.expect(sinkExt.getIncrMode()).andReturn(updateMode);
//            sinkExt.incrMode = updateMode;
//            List<ISelectedTab.ColMeta> metaCols = Lists.newArrayList();
//            ISelectedTab.ColMeta cm = new ISelectedTab.ColMeta();
//            cm.setName(colEntityId);
//            cm.setType(new DataType(Types.VARCHAR, "VARCHAR", 6));
//            metaCols.add(cm);
//
//            cm = new ISelectedTab.ColMeta();
//            cm.setName(colNum);
//            cm.setType(new DataType(Types.INTEGER));
//            metaCols.add(cm);
//
//            cm = new ISelectedTab.ColMeta();
//            cm.setName(colId);
//            cm.setType(new DataType(Types.VARCHAR, "VARCHAR", 32));
//            cm.setPk(true);
//            metaCols.add(cm);
//
//            cm = new ISelectedTab.ColMeta();
//            cm.setName(colCreateTime);
//            cm.setType(new DataType(Types.BIGINT));
//            metaCols.add(cm);
//
//            cm = new ISelectedTab.ColMeta();
//            cm.setName(updateTime);
//            cm.setPk(true);
//            cm.setType(new DataType(Types.TIMESTAMP));
//            metaCols.add(cm);
//
//            cm = new ISelectedTab.ColMeta();
//            cm.setName(updateDate);
//            cm.setType(new DataType(Types.DATE));
//            metaCols.add(cm);
//
//            cm = new ISelectedTab.ColMeta();
//            cm.setName(starTime);
//            cm.setType(new DataType(Types.TIMESTAMP));
//            metaCols.add(cm);
//
//            SelectedTab totalpayInfo = new SelectedTab() {
//                @Override
//                public List<ColMeta> getCols() {
//                    return metaCols;
//                }
//            };
//            totalpayInfo.setIncrSinkProps(sinkExt);
//            totalpayInfo.name = tableName;
//
//
////            EasyMock.expect(sinkExt.getCols()).andReturn(metaCols).times(3);
//            selectedTabs.add(totalpayInfo);
//            EasyMock.expect(dataxReader.getSelectedTabs()).andReturn(selectedTabs).anyTimes();
//
//            EasyMock.expect(dataxProcessor.getReader(null)).andReturn(dataxReader).anyTimes();
//
//            // BasicDataSourceFactory dsFactory = MySqlContainer.createMySqlDataSourceFactory(new TargetResName(dataXName), MYSQL_CONTAINER);
//            BasicDataXRdbmsWriter dataXWriter = createDataXWriter();
//
//            dataXWriter.autoCreateTable = true;
//            dataXWriter.dataXName = dataXName;
//            // dataXWriter.maxBatchRows = 100;
//            DataxWriter.dataxWriterGetter = (xName) -> {
//                Assert.assertEquals(dataXName, xName);
//                return dataXWriter;
//            };
//
//            // EasyMock.expect(dataXWriter.getDataSourceFactory()).andReturn(sourceFactory);
//
//            //   dataXWriter.initWriterTable(tableName, Collections.singletonList("jdbc:mysql://192.168.28.201:9030/tis"));
//
//            EasyMock.expect(dataxProcessor.getWriter(null)).andReturn(dataXWriter);
//
//            ChunjunSinkFactory sinkFactory = getSinkFactory();
//            sinkFactory.setKey(new KeyedPluginStore.Key(null, dataXName, null));
//            sinkFactory.batchSize = 100;
//            sinkFactory.flushIntervalMills = 100000;
//            sinkFactory.semantic = "at-least-once";
//            sinkFactory.parallelism = 1;
//
//
//            Map<String, IDataxProcessor.TableAlias> aliasMap = new HashMap<>();
//            IDataxProcessor.TableAlias tab = new IDataxProcessor.TableAlias(tableName);
//            aliasMap.put(tableName, tab);
//            EasyMock.expect(dataxProcessor.getTabAlias()).andReturn(aliasMap);
//            AtomicInteger httpPutCount = new AtomicInteger();
//            DorisStreamLoad.httpPutConsumer = (httpPut) -> {
//                HttpEntity putEntity = httpPut.getEntity();
//                httpPutCount.incrementAndGet();
//            };
//            this.replay();
//            Map<IDataxProcessor.TableAlias, TabSinkFunc<RowData>> sinkFunction = sinkFactory.createSinkFunction(dataxProcessor);
//            int updateNumVal = 999;
//            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//            DTO add = createDTO(DTO.EventType.ADD);
//            final DTO updateBefore = createDTO(DTO.EventType.UPDATE_BEFORE, (after) -> {
//                after.put(colNum, updateNumVal);
//                after.put(updateTime, "2021-12-17 09:21:22");
//            });
//            final DTO updateAfter = updateBefore.colone();
//            updateAfter.setEventType(DTO.EventType.UPDATE_AFTER);
//
//            final DTO delete = updateBefore.colone();
//            delete.setEventType(DTO.EventType.DELETE);
////            d.setEventType(DTO.EventType.ADD);
////            d.setTableName(tableName);
////            Map<String, Object> after = Maps.newHashMap();
////            after.put(colEntityId, "334556");
////            after.put(colNum, 5);
////            String pk = "88888888887";
////            after.put(colId, pk);
////            after.put(colCreateTime, 20211113115959l);
////            //  after.put(updateTime, "2021-12-17T09:21:20Z");
////            after.put(updateTime, "2021-12-17 09:21:20");
////            after.put(starTime, "2021-12-18 09:21:20");
////            after.put(updateDate, "2021-12-09");
////            d.setAfter(after);
//            Assert.assertEquals(1, sinkFunction.size());
//            for (Map.Entry<IDataxProcessor.TableAlias, TabSinkFunc<RowData>> entry : sinkFunction.entrySet()) {
//
//                DTOStream sourceStream = DTOStream.createDispatched(entry.getKey().getFrom());
//
//                ReaderSource<DTO> readerSource = ReaderSource.createDTOSource("testStreamSource"
//                        , env.fromElements(new DTO[]{add, updateBefore, updateAfter
//                        }));
//
//                readerSource.getSourceStream(env, Collections.singletonMap(tableName, sourceStream));
//
//                entry.getValue().add2Sink(sourceStream);
//
//                //  sourceStream.getStream().addSink();
//
//                // entry.getValue().add2Sink(sourceStream.addStream(env.fromElements(new DTO[]{d, update})));
//                // env.fromElements(new DTO[]{d}).addSink(entry.getValue());
//                break;
//            }
//
//            env.execute("testJob");
//            Thread.sleep(9000);
//            Assert.assertEquals("httpPutCount must be 1", 1, httpPutCount.get());
//
//            DBConfig dbConfig = dsFactory.getDbConfig();
//
//            String[] jdbcUrls = new String[1];
//            dbConfig.vistDbURL(false, (dbName, dbHost, jdbcUrl) -> {
//                jdbcUrls[0] = jdbcUrl;
//            });
//
//            try (Connection conn = dsFactory.getConnection(jdbcUrls[0])) {
//                Statement statement = conn.createStatement();
//                ResultSet resultSet = statement.executeQuery("select * from " + tableName + " where id=" + pk);
//                if (resultSet.next()) {
//                    StringBuffer rowDesc = new StringBuffer();
//                    for (String col : colNames) {
//                        Object obj = resultSet.getObject(col);
//                        rowDesc.append(col).append("=").append(obj).append("[").append((obj != null) ? obj.getClass().getSimpleName() : "").append("]").append(" , ");
//                    }
//                    Assert.assertEquals(updateNumVal, resultSet.getInt(colNum));
//                    System.out.println("test_output==>" + rowDesc.toString());
//                } else {
//                    Assert.fail("have not find row with id=" + pk);
//                }
//            }
//
////            DBConfig dbConfig = dsFactory.getDbConfig();
////            dbConfig.vistDbURL(false, (dbName, dbHost, jdbcUrl) -> {
////                try (Connection conn = dsFactory.getConnection(jdbcUrl)) {
////
////
////
////                } catch (Exception e) {
////                    throw new RuntimeException(e);
////                }
////            });
//            this.verifyAll();
//
//
//        } catch (Throwable e) {
//            Thread.sleep(14000);
//            throw new RuntimeException(e);
//        }
    }

    protected BasicDataXRdbmsWriter createDataXWriter() {
        DataXDorisWriter dataXWriter = new DataXDorisWriter() {
            //                @Override
            //                public DataSourceFactory getDataSourceFactory() {
            //                    return dsFactory;
            //                }

            @Override
            public DorisSourceFactory getDataSourceFactory() {
                //return super.getDataSourceFactory();
                return Objects.requireNonNull(dsFactory, "dsFactory can not be null");
            }
        };

        dataXWriter.loadProps = BasicDorisStarRocksWriter.getDftLoadProps();
        return dataXWriter;
    }

    protected ChunjunSinkFactory getSinkFactory() {
        return new ChunjunDorisSinkFactory();
    }


    @After
    public void afterTestSinkSync() throws Exception {
        environment.stop();
    }


    public static DorisSourceFactory getDorisSourceFactory(String host, int jdbcPort, int loadPort) {
        DorisSourceFactory dataSourceFactory = new DorisSourceFactory();// {
//            @Override
//            protected Connection getConnection(String jdbcUrl, String username, String password) throws SQLException {
//                throw new UnsupportedOperationException();
//            }
        //};

        dataSourceFactory.dbName = null;
        dataSourceFactory.password = null;
        dataSourceFactory.userName = "root";
        dataSourceFactory.nodeDesc = host;
        dataSourceFactory.port = jdbcPort;
        dataSourceFactory.encode = "utf8";
        dataSourceFactory.loadUrl = "[\"" + host + ":" + loadPort + "\"]";
        return dataSourceFactory;
    }
}
