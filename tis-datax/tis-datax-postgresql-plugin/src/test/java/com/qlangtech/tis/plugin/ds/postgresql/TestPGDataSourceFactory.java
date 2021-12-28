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

package com.qlangtech.tis.plugin.ds.postgresql;

import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.plugin.ds.ColumnMetaData;
import junit.framework.TestCase;

import java.util.List;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-06-22 14:15
 **/
public class TestPGDataSourceFactory extends TestCase {
    public void testShowTables() {

        String createSQL = IOUtils.loadResourceFromClasspath(TestPGDataSourceFactory.class, "create-sql-instancedetail.sql");

        // 之后数据库如果没有了可以用下面的这个SQL 再把数据库给跑起来的
        System.out.println(createSQL);

        PGDataSourceFactory dsFactory = new PGDataSourceFactory();
        dsFactory.userName = "postgres";
        dsFactory.password = "123456";
        dsFactory.dbName = "tis";
        dsFactory.port = 5432;
        dsFactory.nodeDesc = "192.168.28.201";
        dsFactory.tabSchema = "public";

        String instancedetail = "instancedetail";
        List<String> tables = dsFactory.getTablesInDB();
        assertEquals(1, tables.size());
        tables.contains(instancedetail);


        List<ColumnMetaData> tableMetadata = dsFactory.getTableMetadata(instancedetail);
        assertTrue(tableMetadata.size() > 0);

        for (ColumnMetaData col : tableMetadata) {
            System.out.println(col.getKey() + "  " + col.getType());
        }
    }
}
