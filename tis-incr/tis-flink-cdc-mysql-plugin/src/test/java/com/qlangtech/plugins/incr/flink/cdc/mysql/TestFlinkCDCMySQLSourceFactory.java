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

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;
import com.qlangtech.plugins.incr.flink.TISFlinClassLoaderFactory;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.manage.common.CenterResource;
import com.qlangtech.tis.manage.common.TisUTF8;
import com.qlangtech.tis.plugin.datax.SelectedTab;
import com.qlangtech.tis.plugin.datax.common.BasicDataXRdbmsReader;
import com.qlangtech.tis.plugin.datax.common.RdbmsReaderContext;
import com.qlangtech.tis.plugin.ds.*;
import com.qlangtech.tis.plugin.incr.TISSinkFactory;
import com.qlangtech.tis.realtime.BasicFlinkSourceHandle;
import com.qlangtech.tis.realtime.transfer.DTO;
import com.qlangtech.tis.test.TISEasyMock;
import com.qlangtech.tis.util.IPluginContext;
import com.ververica.cdc.connectors.mysql.testutils.MySqlContainer;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.test.util.AbstractTestBase;
import org.junit.*;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-12-18 09:57
 **/
public class TestFlinkCDCMySQLSourceFactory extends AbstractTestBase implements TISEasyMock {
    private static final Logger LOG = LoggerFactory.getLogger(TestFlinkCDCMySQLSourceFactory.class);

    @ClassRule(order = 100)
    public static TestRule name = new TestRule() {
        @Override
        public Statement apply(Statement base, Description description) {
            System.setProperty(TISFlinClassLoaderFactory.SKIP_CLASSLOADER_FACTORY_CREATION, "true");
            return base;
        }
    };

    //protected static final int DEFAULT_PARALLELISM = 4;
    protected static final MySqlContainer MYSQL_CONTAINER =
            (MySqlContainer)
                    new MySqlContainer()
                            .withConfigurationOverride("docker/server-gtids/my.cnf")
                            .withSetupSQL("docker/setup.sql")
                            .withDatabaseName("flink-test")
                            .withUsername("flinkuser")
                            .withPassword("flinkpw")
                            .withLogConsumer(new Slf4jLogConsumer(LOG) {
                                @Override
                                public void accept(OutputFrame outputFrame) {
                                    OutputFrame.OutputType outputType = outputFrame.getType();
                                    String utf8String = outputFrame.getUtf8String();
                                    System.out.println(utf8String);
                                    super.accept(outputFrame);
                                }
                            });

//    @Rule
//    public final MiniClusterWithClientResource miniClusterResource =
//            new MiniClusterWithClientResource(
//                    new MiniClusterResourceConfiguration.Builder()
//                            .setNumberTaskManagers(1)
//                            .setNumberSlotsPerTaskManager(DEFAULT_PARALLELISM)
//                            .setRpcServiceSharing(RpcServiceSharing.DEDICATED)
//                            .withHaLeadershipControl()
//                            .build());

    @BeforeClass
    public static void startContainers() {
        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(MYSQL_CONTAINER)).join();
        LOG.info("Containers are started.");
    }

    @Before
    public void setUp() throws Exception {
        CenterResource.setNotFetchFromCenterRepository();
    }


    @Test
    public void testBinlogConsume() throws Exception {

        //  miniClusterResource.getRestAddres();
        FlinkCDCMySQLSourceFactory mysqlCDCFactory = new FlinkCDCMySQLSourceFactory();
        mysqlCDCFactory.startupOptions = "latest";
        final String tabName = "base";
        TargetResName dataxName = new TargetResName("x");

        BasicFlinkSourceHandle consumerHandle = new TestBasicFlinkSourceHandle(tabName);

        TISSinkFactory sinkFuncFactory = new TISSinkFactory() {
            @Override
            public Map<IDataxProcessor.TableAlias, SinkFunction<DTO>> createSinkFunction(IDataxProcessor dataxProcessor) {
                Map<IDataxProcessor.TableAlias, SinkFunction<DTO>> result = Maps.newHashMap();
                result.put(new IDataxProcessor.TableAlias(tabName), new PrintSinkFunction());
                return result;
            }
        };
        consumerHandle.setSinkFuncFactory(sinkFuncFactory);
        mysqlCDCFactory.setConsumerHandle(consumerHandle);

        FlinkCDCMysqlSourceFunction imqListener = (FlinkCDCMysqlSourceFunction) mysqlCDCFactory.create();

        // DataxReader.IDataxReaderGetter readerGetter = mock("IDataxReaderGetter", DataxReader.IDataxReaderGetter.class);


        Descriptor mySqlV5DataSourceFactory = TIS.get().getDescriptor("MySQLV5DataSourceFactory");
        Assert.assertNotNull(mySqlV5DataSourceFactory);

        FormData formData = new FormData();
        formData.addProp("name", "mysql");
        formData.addProp("dbName", MYSQL_CONTAINER.getDatabaseName());
        formData.addProp("nodeDesc", MYSQL_CONTAINER.getHost());
        formData.addProp("password", MYSQL_CONTAINER.getPassword());
        formData.addProp("userName", MYSQL_CONTAINER.getUsername());
        formData.addProp("port", String.valueOf(MYSQL_CONTAINER.getDatabasePort()));
        formData.addProp("encode", "utf8");
        formData.addProp("useCompression", "true");

        Descriptor.ParseDescribable<BasicDataSourceFactory> parseDescribable
                = mySqlV5DataSourceFactory.newInstance(IPluginContext.namedContext(dataxName.getName()), formData, Optional.empty());
        Assert.assertNotNull(parseDescribable.instance);

        BasicDataSourceFactory dataSourceFactory = parseDescribable.instance;
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


        IDataxProcessor dataXProcessor = mock("dataXProcessor", IDataxProcessor.class);

        replay();
        List<SelectedTab> selectedTabs = dataxReader.getSelectedTabs();
        Optional<SelectedTab> firstSelectedTab
                = selectedTabs.stream().filter((t) -> tabName.equals(t.name)).findFirst();
        Assert.assertTrue("firstSelectedTab:" + tabName + " must be present", firstSelectedTab.isPresent());

        ISelectedTab tab = firstSelectedTab.get();
        List<ISelectedTab> tabs = Collections.singletonList(tab);

        Runnable aid = () -> {
            dataSourceFactory.visitFirstConnection((conn) -> {

//                `base_id` int(11) NOT NULL,
//                `start_time` datetime DEFAULT NULL,
//                        `update_date` date DEFAULT NULL,
//                        `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
//                        `price` decimal(5,2) DEFAULT NULL,
//                `json_content` json DEFAULT NULL,
//                        `col_blob` blob,
//                        `col_text` text,

                String insertBase
                        = "insert into base(`base_id` ,`start_time`,`update_date`,`update_time`,`price`, `json_content`, `col_blob` , `col_text`) " +
                        "values(" +
                        "?,?,?,?,?,?,?,?)";

                PreparedStatement statement = conn.prepareStatement(insertBase);
                int id = 66;

                while (id < 88) {
                    statement.setInt(1, id++);
                    statement.setDate(2, new java.sql.Date(System.currentTimeMillis()));
                    statement.setDate(3, new java.sql.Date(System.currentTimeMillis()));
                    statement.setDate(4, new java.sql.Date(System.currentTimeMillis()));
                    statement.setBigDecimal(5, BigDecimal.valueOf(199, 2));
                    statement.setString(6, "{\"name\":\"baisui\"}");

                    try {
                        try (ByteArrayInputStream blob = new ByteArrayInputStream("Hello world".getBytes(TisUTF8.get()))) {
                            statement.setBlob(7, blob);
                        }
                    } catch (IOException e) {
                        throw new SQLException(e);
                    }
                    statement.setString(8, "我爱北京天安门");
                    Assert.assertEquals(1, statement.executeUpdate());
                    System.out.println("insert:" + id);
                    try {
                        Thread.sleep(500);
                    } catch (Exception e) {
                    }
                }
                statement.close();
            });
        };

        (new Thread(aid)).start();

        imqListener.start(dataxName, dataxReader, tabs, dataXProcessor);
        Thread.sleep(10000);
        verifyAll();
    }

    private static class FormData extends HashMap<String, JSONObject> {

        public JSONObject addProp(String key, String val) {
            JSONObject o = new JSONObject();
            o.put(Descriptor.KEY_primaryVal, val);
            this.put(key, o);
            return o;
        }
    }
}
