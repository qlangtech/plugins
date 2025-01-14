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

package com.qlangtech.tis.plugin.ds.kingbase;

import com.qlangtech.tis.plugin.ds.ColumnMetaData;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-01-14 14:53
 **/
public class TestKingBaseDataSourceFactory {

    @Test
    public void testGetTableMetadata() {
        KingBaseDataSourceFactory dataSourceFactory = new KingBaseDataSourceFactory();
        dataSourceFactory.name = "kingbase201";
        dataSourceFactory.nodeDesc = "192.168.28.201";
        dataSourceFactory.port = 4321;
        dataSourceFactory.dbName = "test";
        dataSourceFactory.userName = "kingbase";
        dataSourceFactory.password = "123456";
        dataSourceFactory.tabSchema = "public";
        EntityName orderdetail = EntityName.parse("public.orderdetail");
        List<ColumnMetaData> tableMetadata = dataSourceFactory.getTableMetadata(true, orderdetail);
        Assert.assertNotNull(tableMetadata);
    }
}
