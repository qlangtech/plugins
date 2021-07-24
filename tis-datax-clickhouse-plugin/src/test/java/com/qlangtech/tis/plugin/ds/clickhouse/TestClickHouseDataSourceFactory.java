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

package com.qlangtech.tis.plugin.ds.clickhouse;

import com.qlangtech.tis.plugin.common.PluginDesc;
import junit.framework.TestCase;

import java.util.List;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-06-18 09:34
 **/
public class TestClickHouseDataSourceFactory extends TestCase {

    public void testDescGenerate() {

        PluginDesc.testDescGenerate(ClickHouseDataSourceFactory.class, "ClickHouseDataSourceFactory-desc.json");
    }

    public void testGetTablesInDB() {
        ClickHouseDataSourceFactory dsFactory = new ClickHouseDataSourceFactory();
        dsFactory.nodeDesc = "192.168.28.201";
        dsFactory.port = 8123;
        dsFactory.dbName = "tis";
        dsFactory.name = "tis-clickhouse";
        dsFactory.userName = "default";
        dsFactory.password = "123456";

        List<String> tablesInDB = dsFactory.getTablesInDB();
        assertTrue(tablesInDB.size() > 0);
    }

}
