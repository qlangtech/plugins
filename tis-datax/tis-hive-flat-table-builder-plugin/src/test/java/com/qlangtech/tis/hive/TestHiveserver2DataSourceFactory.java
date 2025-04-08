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

package com.qlangtech.tis.hive;

import com.qlangtech.tis.config.authtoken.UserToken;
import com.qlangtech.tis.config.authtoken.UserTokenUtils;
import com.qlangtech.tis.config.hive.IHiveConnGetter;
import com.qlangtech.tis.config.hive.meta.HiveTable;
import com.qlangtech.tis.config.hive.meta.IHiveMetaStore;
import com.qlangtech.tis.plugin.ds.DataSourceMeta;
import com.qlangtech.tis.plugin.ds.JDBCConnection;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-07-20 15:59
 **/
public class TestHiveserver2DataSourceFactory {

    @Test
    public void testGetAlternativeHdfsSubPath() {
        Hiveserver2DataSourceFactory hive2DataSourceFactory = new Hiveserver2DataSourceFactory();
        hive2DataSourceFactory.dbName = "order";
        hive2DataSourceFactory.alternativeHdfsSubPath = "$1.db";

        Assert.assertEquals("order.db", hive2DataSourceFactory.getAlternativeHdfsSubPath());

        hive2DataSourceFactory.alternativeHdfsSubPath = null;

        Assert.assertEquals("order", hive2DataSourceFactory.getAlternativeHdfsSubPath());

        hive2DataSourceFactory.alternativeHdfsSubPath = "dezhou.db";

        Assert.assertEquals("dezhou.db", hive2DataSourceFactory.getAlternativeHdfsSubPath());
    }


    @Test
    public void testCreateConnWithKerberOs() throws Exception {
        //  Hiveserver2DataSourceFactory hsource = new Hiveserver2DataSourceFactory();

        String jdbcUrl = IHiveConnGetter.HIVE2_JDBC_SCHEMA + "192.168.28.200:10000" + "/default";

        UserToken userToken = createKerberToken();

        // UserToken userToken = new Kerber;
        try (JDBCConnection conn = Hms.createConnection(jdbcUrl, userToken)) {
            Assert.assertNotNull(conn);

            conn.query("show tables", (result) -> {
                System.out.println(result.getString(1));
                return true;
            });
        }
    }

    private UserToken createKerberToken() {
        return UserTokenUtils.createKerberosToken("k2");
//        Descriptor.FormData kform = new Descriptor.FormData();
//        kform.addProp("kerberos", "k2");
//        Descriptor kuserTokenDesc = TIS.get().getDescriptor("com.qlangtech.tis.config.authtoken.impl.KerberosUserToken");
//        Assert.assertNotNull("kuserTokenDesc can not be null", kuserTokenDesc);
//        return (UserToken) kuserTokenDesc.newInstance("test", kform).getInstance();
    }

    @Test
    public void testCreateMetaWithKerberOs() throws Exception {
        //  Hiveserver2DataSourceFactory hsource = new Hiveserver2DataSourceFactory();

        Hiveserver2DataSourceFactory hiveDS = new Hiveserver2DataSourceFactory();
        HiveMeta meta = new HiveMeta();
        meta.metaStoreUrls = "thrift://47.96.7.122:9083";
        meta.userToken = createKerberToken(); //new OffUserToken();
        //  hiveDS.metadata = createKerberToken();
        hiveDS.metadata = meta;
        //  hiveDS.userToken = createKerberToken();

        try (IHiveMetaStore metaStore = hiveDS.createMetaStoreClient()) {
            Assert.assertNotNull("metaStore can not be null", metaStore);
            List<HiveTable> tabs = metaStore.getTables("default");

            Assert.assertNotNull("tabs can not be null", tabs);
            for (HiveTable tab : tabs) {
                System.out.println(tab.getTableName());
            }
        }

    }
}
