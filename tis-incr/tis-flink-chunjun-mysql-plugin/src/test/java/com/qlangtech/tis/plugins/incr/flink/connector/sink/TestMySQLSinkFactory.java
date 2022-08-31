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

package com.qlangtech.tis.plugins.incr.flink.connector.sink;

import com.dtstack.chunjun.conf.SyncConf;
import com.qlangtech.plugins.incr.flink.cdc.mysql.MySqlSourceTestBase;
import com.qlangtech.plugins.incr.flink.chunjun.doris.sink.TestFlinkSinkExecutor;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.datax.impl.DataxReader;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.plugin.datax.DataxMySQLWriter;
import com.qlangtech.tis.plugin.datax.common.BasicDataXRdbmsWriter;
import com.qlangtech.tis.plugin.ds.BasicDataSourceFactory;
import com.qlangtech.tis.plugin.ds.DataSourceFactory;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugins.incr.flink.connector.ChunjunSinkFactory;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-07-17 14:54
 * @see MySqlSourceTestBase
 **/
public class TestMySQLSinkFactory extends TestFlinkSinkExecutor
        //        implements TISEasyMock
{

    @Test
    public void testMySQLWrite() throws Exception {
        super.testSinkSync();
    }

    // String dataXName = "testDataX";

//    String tableName = "totalpayinfo";
//
//    String colEntityId = "entity_id";
//    String colNum = "num";
//    String colId = "id";
//    String colCreateTime = "create_time";
//    String updateTime = "update_time";
//    String updateDate = "update_date";
//    String starTime = "start_time";
//
//    String pk = "88888888887";

//    @Rule
//    public TemporaryFolder folder = new TemporaryFolder();


    static BasicDataSourceFactory mysqlDSFactory;

    @Override
    protected DataxReader createDataxReader() {
        DataxReader dataxReader = super.createDataxReader();
        DataxReader.dataxReaderThreadLocal.set(dataxReader);
        return dataxReader;
    }

    @BeforeClass
    public static void initialize() throws Exception {
        MySqlSourceTestBase.startContainers();
        mysqlDSFactory = MySqlSourceTestBase.createDataSource(new TargetResName(dataXName));
    }


    @Test
    public void testConfParse() {
        SyncConf syncConf = SyncConf.parseJob(IOUtils.loadResourceFromClasspath(MySQLSinkFactory.class, "mysql_mysql_batch.json"));
        Assert.assertNotNull(syncConf);
    }

    @Override
    protected ISelectedTab.ColMeta createUpdateTime() {
        ISelectedTab.ColMeta updateTime = super.createUpdateTime();
        updateTime.setPk(false);
        return updateTime;
    }

    @Override
    protected BasicDataSourceFactory getDsFactory() {
        return mysqlDSFactory;
    }

    @Override
    protected ChunjunSinkFactory getSinkFactory() {
        MySQLSinkFactory sinkFactory = new MySQLSinkFactory();
        return sinkFactory;
    }

    @Override
    protected BasicDataXRdbmsWriter createDataXWriter() {
        DataxMySQLWriter dataXWriter = new DataxMySQLWriter() {
            @Override
            public DataSourceFactory getDataSourceFactory() {
                return mysqlDSFactory;
            }
        };

        return dataXWriter;
    }

//    @Test
//    public void testStartRocksWrite() throws Exception {
//        super.testSinkSync();

//        System.out.println(this.getClass().getResource("/org/apache/logging/slf4j/Log4jLogger.class"));
//        /**
//         CREATE TABLE `totalpayinfo` (
//         `id` varchar(32) NULL COMMENT "",
//         `entity_id` varchar(10) NULL COMMENT "",
//         `num` int(11) NULL COMMENT "",
//         `create_time` bigint(20) NULL COMMENT "",
//         `update_time` DATETIME   NULL,
//         `update_date` DATE       NULL,
//         `start_time`  DATETIME   NULL
//         ) ENGINE=OLAP
//         UNIQUE KEY(`id`)
//         DISTRIBUTED BY HASH(`id`) BUCKETS 10
//         PROPERTIES (
//         "replication_num" = "1",
//         "in_memory" = "false",
//         "storage_format" = "DEFAULT"
//         );
//         * */
//
//        try {
//
//
//            String[] colNames = new String[]{colEntityId, colNum, colId, colCreateTime, updateTime, updateDate, starTime};
//
//            DataxProcessor dataxProcessor = mock("dataxProcessor", DataxProcessor.class);
//
//            File ddlDir = folder.newFolder("ddl");
//            String tabSql = tableName + IDataxProcessor.DATAX_CREATE_DDL_FILE_NAME_SUFFIX;
//            FileUtils.write(new File(ddlDir, tabSql)
//                    , IOUtils.loadResourceFromClasspath(MySQLSinkFactory.class, tabSql), TisUTF8.get());
//
//            EasyMock.expect(dataxProcessor.getDataxCreateDDLDir(null)).andReturn(ddlDir);
//
//            DataxProcessor.processorGetter = (name) -> {
//                return dataxProcessor;
//            };
//            IDataxReader dataxReader = mock("dataxReader", IDataxReader.class);
//            List<ISelectedTab> selectedTabs = Lists.newArrayList();
//            SinkTabPropsExtends totalpayinfo = mock(tableName, SinkTabPropsExtends.class);
//            EasyMock.expect(totalpayinfo.tabName).andReturn(tableName).times(2);
//            //InsertType updateMode = new InsertType();
//
//            ReplaceType updateMode = new ReplaceType();
//            updateMode.updateKey = Collections.singletonList(colId);
//            EasyMock.expect(totalpayinfo.getIncrMode()).andReturn(updateMode);
//            List<ISelectedTab.ColMeta> cols = Lists.newArrayList();
//            ISelectedTab.ColMeta cm = new ISelectedTab.ColMeta();
//            cm.setName(colEntityId);
//            cm.setType(new DataType(Types.VARCHAR, "VARCHAR", 6));
//            cols.add(cm);
//
//            cm = new ISelectedTab.ColMeta();
//            cm.setName(colNum);
//            cm.setType(new DataType(Types.INTEGER));
//            cols.add(cm);
//
//            cm = new ISelectedTab.ColMeta();
//            cm.setName(colId);
//            cm.setType(new DataType(Types.VARCHAR, "VARCHAR", 32));
//            cm.setPk(true);
//            cols.add(cm);
//
//            cm = new ISelectedTab.ColMeta();
//            cm.setName(colCreateTime);
//            cm.setType(new DataType(Types.BIGINT));
//            cols.add(cm);
//
//            cm = new ISelectedTab.ColMeta();
//            cm.setName(updateTime);
//            cm.setType(new DataType(Types.TIMESTAMP));
//            cols.add(cm);
//
//            cm = new ISelectedTab.ColMeta();
//            cm.setName(updateDate);
//            cm.setType(new DataType(Types.DATE));
//            cols.add(cm);
//
//            cm = new ISelectedTab.ColMeta();
//            cm.setName(starTime);
//            cm.setType(new DataType(Types.TIMESTAMP));
//            cols.add(cm);
//
//            EasyMock.expect(totalpayinfo.getCols()).andReturn(cols).times(3);
//            selectedTabs.add(totalpayinfo);
//            EasyMock.expect(dataxReader.getSelectedTabs()).andReturn(selectedTabs).anyTimes();
//
//            EasyMock.expect(dataxProcessor.getReader(null)).andReturn(dataxReader).anyTimes();
//
//            BasicDataSourceFactory dsFactory = MySqlContainer.createMySqlDataSourceFactory(new TargetResName(dataXName), MYSQL_CONTAINER);
//            DataxMySQLWriter dataXWriter = new DataxMySQLWriter() {
//                @Override
//                public DataSourceFactory getDataSourceFactory() {
//                    return dsFactory;
//                }
//            };
//            dataXWriter.autoCreateTable = true;
//            dataXWriter.dataXName = dataXName;
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
//            MySQLSinkFactory sinkFactory = new MySQLSinkFactory();
//            sinkFactory.setKey(new KeyedPluginStore.Key(null, dataXName, null));
//            sinkFactory.batchSize = 1;
//            sinkFactory.semantic = "at-least-once";
//
//
//            Map<String, IDataxProcessor.TableAlias> aliasMap = new HashMap<>();
//            IDataxProcessor.TableAlias tab = new IDataxProcessor.TableAlias(tableName);
//            aliasMap.put(tableName, tab);
//            EasyMock.expect(dataxProcessor.getTabAlias()).andReturn(aliasMap);
//
//            this.replay();
//            Map<IDataxProcessor.TableAlias, TabSinkFunc<RowData>> sinkFunction = sinkFactory.createSinkFunction(dataxProcessor);
//
//            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//            DTO d = createDTO(DTO.EventType.ADD);
//            DTO update = createDTO(DTO.EventType.UPDATE_AFTER, (after) -> {
//                after.put(colNum, 999);
//            });
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
//                sourceStream.addStream(env.fromElements(new DTO[]{d, update}));
//
//                entry.getValue().add2Sink(sourceStream);
//                // env.fromElements(new DTO[]{d}).addSink(entry.getValue());
//                break;
//            }
//
//            env.execute("testJob");
//            Thread.sleep(14000);
//            DBConfig dbConfig = dsFactory.getDbConfig();
//            dbConfig.vistDbURL(false, (dbName, dbHost, jdbcUrl) -> {
//                try (Connection conn = dsFactory.getConnection(jdbcUrl)) {
//
//                    Statement statement = conn.createStatement();
//                    ResultSet resultSet = statement.executeQuery("select * from " + tableName + " where id=" + pk);
//                    if (resultSet.next()) {
//                        StringBuffer rowDesc = new StringBuffer();
//                        for (String col : colNames) {
//                            Object obj = resultSet.getObject(col);
//                            rowDesc.append(col).append("=").append(obj).append("[").append(obj.getClass().getSimpleName()).append("]").append(" , ");
//                        }
//                        System.out.println(rowDesc.toString());
//                    } else {
//                        Assert.fail("have not find row with id=" + pk);
//                    }
//
//                } catch (Exception e) {
//                    throw new RuntimeException(e);
//                }
//            });
//            this.verifyAll();
//        } catch (Throwable e) {
//            Thread.sleep(14000);
//            throw new RuntimeException(e);
//        }
    //  }

//    private DTO createDTO(DTO.EventType eventType, Consumer<Map<String, Object>>... consumer) {
//        DTO d = new DTO();
//        d.setEventType(eventType);
//        d.setTableName(tableName);
//        Map<String, Object> after = Maps.newHashMap();
//        after.put(colEntityId, "334556");
//        after.put(colNum, 5);
//        after.put(colId, pk);
//        after.put(colCreateTime, 20211113115959l);
//        //  after.put(updateTime, "2021-12-17T09:21:20Z");
//        after.put(updateTime, "2021-12-17 09:21:20");
//        after.put(starTime, "2021-12-18 09:21:20");
//        after.put(updateDate, "2021-12-09");
//        d.setAfter(after);
//        if (eventType != DTO.EventType.ADD) {
//            d.setBefore(Maps.newHashMap(after));
//            for (Consumer<Map<String, Object>> c : consumer) {
//                c.accept(after);
//            }
//        }
//        return d;
//    }
}
