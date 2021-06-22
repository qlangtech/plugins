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

package com.qlangtech.tis.plugin.ds.cassandra;

import com.google.common.collect.Sets;
import com.qlangtech.tis.plugin.ds.ColumnMetaData;
import junit.framework.TestCase;

import java.util.List;
import java.util.Set;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-06-21 13:14
 **/
public class TestCassandraDatasourceFactory extends TestCase {

    public void testGetTablesInDB() {
        CassandraDatasourceFactory dsFactory = getDS();
        // dsFactory.useSSL = true;

        List<String> tablesInDB = dsFactory.getTablesInDB();
        assertTrue(tablesInDB.contains("user_dtl"));
    }

    public static CassandraDatasourceFactory getDS() {
        CassandraDatasourceFactory dsFactory = new CassandraDatasourceFactory();
        dsFactory.dbName = "test_ks";
        dsFactory.nodeDesc = "192.168.28.201";
        dsFactory.password = "cassandra";
        dsFactory.userName = "cassandra";
        dsFactory.port = 9042;
        return dsFactory;
    }

    public void testGetTableMetadata() {
        Set<String> keys = Sets.newHashSet("city", "user_id", "user_name");
        CassandraDatasourceFactory ds = getDS();
        List<ColumnMetaData> colsMeta = ds.getTableMetadata("user_dtl");
        assertEquals(3, colsMeta.size());
        for (ColumnMetaData col : colsMeta) {
            assertTrue(keys.contains(col.getKey()));
        }
    }
}
