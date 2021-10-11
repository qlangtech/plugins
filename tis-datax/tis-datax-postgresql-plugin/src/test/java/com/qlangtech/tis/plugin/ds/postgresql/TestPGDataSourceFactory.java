/**
 * Copyright (c) 2020 QingLang, Inc. <baisui@qlangtech.com>
 * <p>
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 * <p>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
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
