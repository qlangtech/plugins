/**
 * Copyright (c) 2020 QingLang, Inc. <baisui@qlangtech.com>
 * <p>
 *   This program is free software: you can use, redistribute, and/or modify
 *   it under the terms of the GNU Affero General Public License, version 3
 *   or later ("AGPL"), as published by the Free Software Foundation.
 * <p>
 *  This program is distributed in the hope that it will be useful, but WITHOUT
 *  ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 *   FITNESS FOR A PARTICULAR PURPOSE.
 * <p>
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.qlangtech.tis.plugin.ds.tidb;

import com.qlangtech.tis.plugin.ds.ColumnMetaData;
import junit.framework.TestCase;

import java.util.List;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-04-23 19:16
 **/
public class GetColsMeta {
    private TiKVDataSourceFactory dataSourceFactory;
    private List<ColumnMetaData> employeesCols;

    public TiKVDataSourceFactory getDataSourceFactory() {
        return dataSourceFactory;
    }

    public List<ColumnMetaData> getEmployeesCols() {
        return employeesCols;
    }

    public GetColsMeta invoke() {
        return invoke(true);
    }

    public GetColsMeta invoke(boolean datetimeFormat) {
        dataSourceFactory = new TiKVDataSourceFactory();
        dataSourceFactory.dbName = TestTiKVDataSourceFactory.DB_NAME;
        dataSourceFactory.pdAddrs = "192.168.28.202:2379";
        dataSourceFactory.datetimeFormat = datetimeFormat;
        employeesCols = dataSourceFactory.getTableMetadata(TestTiKVDataSourceFactory.TABLE_NAME);
        TestCase.assertNotNull(employeesCols);
        TestCase.assertEquals(6, employeesCols.size());
        return this;
    }
}
