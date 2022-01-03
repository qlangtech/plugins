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

import com.google.common.collect.Maps;
import com.qlangtech.plugins.incr.flink.TISFlinClassLoaderFactory;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.manage.common.CenterResource;
import com.qlangtech.tis.plugin.datax.SelectedTab;
import com.qlangtech.tis.plugin.datax.common.BasicDataXRdbmsReader;
import com.qlangtech.tis.plugin.ds.BasicDataSourceFactory;
import com.qlangtech.tis.plugin.ds.DBConfig;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.incr.TISSinkFactory;
import com.qlangtech.tis.realtime.BasicFlinkSourceHandle;
import com.qlangtech.tis.realtime.transfer.DTO;
import com.qlangtech.tis.test.TISEasyMock;
import com.ververica.cdc.connectors.mysql.testutils.MySqlContainer;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.easymock.EasyMock;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-12-18 09:57
 **/
public class TestFlinkCDCMySQLSourceFactory implements TISEasyMock {
    private static final Logger LOG = LoggerFactory.getLogger(TestFlinkCDCMySQLSourceFactory.class);
    protected static final int DEFAULT_PARALLELISM = 4;
    protected static final MySqlContainer MYSQL_CONTAINER =
            (MySqlContainer)
                    new MySqlContainer()
                            .withConfigurationOverride("docker/server-gtids/my.cnf")
                            .withSetupSQL("docker/setup.sql")
                            .withDatabaseName("flink-test")
                            .withUsername("flinkuser")
                            .withPassword("flinkpw")
                            .withLogConsumer(new Slf4jLogConsumer(LOG));

    @Rule
    public final MiniClusterWithClientResource miniClusterResource =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(DEFAULT_PARALLELISM)
                            .setRpcServiceSharing(RpcServiceSharing.DEDICATED)
                            .withHaLeadershipControl()
                            .build());

    @BeforeClass
    public static void startContainers() {
        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(MYSQL_CONTAINER)).join();
        LOG.info("Containers are started.");
    }

    @Before
    public void setUp() throws Exception {
        CenterResource.setNotFetchFromCenterRepository();
        System.setProperty(TISFlinClassLoaderFactory.SKIP_CLASSLOADER_FACTORY_CREATION, "true");
    }


    @Test
    public void testBinlogConsume() throws Exception {


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
        BasicDataXRdbmsReader dataxReader = this.mock("dataxReader", BasicDataXRdbmsReader.class);

        BasicDataSourceFactory dataSourceFactory = new BasicDataSourceFactory() {
            @Override
            public String buidJdbcUrl(DBConfig db, String ip, String dbName) {
                // return MYSQL_CONTAINER.getJdbcUrl();
                throw new UnsupportedOperationException();
            }
        };

        dataSourceFactory.dbName = MYSQL_CONTAINER.getDatabaseName();
        dataSourceFactory.nodeDesc = MYSQL_CONTAINER.getHost();
        dataSourceFactory.password = MYSQL_CONTAINER.getPassword();
        dataSourceFactory.userName = MYSQL_CONTAINER.getUsername();
        dataSourceFactory.port = MYSQL_CONTAINER.getDatabasePort();

        EasyMock.expect(dataxReader.getDataSourceFactory()).andReturn(dataSourceFactory);
        // EasyMock.expect(readerGetter.get(dataxName.getName())).andReturn(dataxReader);

        //  DataxReader.dataxReaderGetter = readerGetter;
        // BasicDataXRdbmsReader dataxReader = (BasicDataXRdbmsReader) DataxReader.load(null, dataxName.getName());
        // Objects.requireNonNull(dataxReader);
//        IDataxReader rdbmsReader =

        List<SelectedTab> selectedTabs = dataxReader.getSelectedTabs();
        Optional<SelectedTab> firstSelectedTab = selectedTabs.stream().filter((t) -> tabName.equals(t.name)).findFirst();
        Assert.assertTrue("firstSelectedTab must be present", firstSelectedTab.isPresent());

        ISelectedTab tab = firstSelectedTab.get();


        List<ISelectedTab> tabs = Collections.singletonList(tab);
        IDataxProcessor dataXProcessor = mock("dataXProcessor", IDataxProcessor.class);
        replay();
        imqListener.start(dataxName, dataxReader, tabs, dataXProcessor);
        Thread.sleep(10000);
        verifyAll();
    }
}
