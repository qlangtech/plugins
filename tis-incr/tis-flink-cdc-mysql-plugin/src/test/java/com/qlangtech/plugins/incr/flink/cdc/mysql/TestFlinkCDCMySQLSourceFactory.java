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

import com.qlangtech.plugins.incr.flink.cdc.CUDCDCTestSuit;
import com.qlangtech.plugins.incr.flink.cdc.IResultRows;
import com.qlangtech.plugins.incr.flink.cdc.source.TestTableRegisterFlinkSourceHandle;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.manage.common.CenterResource;
import com.qlangtech.tis.plugin.ds.BasicDataSourceFactory;
import com.qlangtech.tis.test.TISEasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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
