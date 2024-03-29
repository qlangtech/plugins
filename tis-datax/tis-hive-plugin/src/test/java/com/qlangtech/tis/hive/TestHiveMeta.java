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

import com.qlangtech.tis.config.authtoken.impl.OffUserToken;
import com.qlangtech.tis.config.hive.meta.HiveTable;
import com.qlangtech.tis.config.hive.meta.IHiveMetaStore;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-08-19 14:21
 **/
public class TestHiveMeta {

    @Test
    public void testCreateMetaStoreClient() throws Exception {

        HiveMeta hiveMeta = new HiveMeta();
        hiveMeta.metaStoreUrls = "thrift://192.168.28.200:9083";
        hiveMeta.userToken = new OffUserToken();
        IHiveMetaStore metaClient = hiveMeta.createMetaStoreClient();

        String tabName = "instancedetail";
        HiveTable table = metaClient.getTable("default", "instancedetail");

        Assert.assertEquals(tabName, table.getTableName());

    }
}
