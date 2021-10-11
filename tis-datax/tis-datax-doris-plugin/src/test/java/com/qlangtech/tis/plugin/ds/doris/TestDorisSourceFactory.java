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

package com.qlangtech.tis.plugin.ds.doris;

import com.qlangtech.tis.manage.common.CenterResource;
import com.qlangtech.tis.manage.common.HttpUtils;
import com.qlangtech.tis.plugin.ds.DataDumpers;
import com.qlangtech.tis.plugin.ds.IDataSourceDumper;
import com.qlangtech.tis.plugin.ds.TISTable;
import junit.framework.TestCase;

import java.util.Iterator;
import java.util.Map;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-09-07 14:01
 **/
public class TestDorisSourceFactory extends TestCase {

    private static final String DB_ORDER = "order1";
    private static final String empNo = "emp_no";

    @Override
    public void setUp() throws Exception {
        super.setUp();
        CenterResource.setNotFetchFromCenterRepository();
        HttpUtils.addMockGlobalParametersConfig();
    }


    public void testDataDumpers() throws Exception {
        DorisSourceFactory dataSourceFactory = getDorisSourceFactory();
        TISTable dumpTable = new TISTable();
        dumpTable.setSelectSql("SELECT emp_no,birth_date,first_name,last_name,gender,hire_date FROM employees");
        dumpTable.setTableName("employees");
        DataDumpers dataDumpers = dataSourceFactory.getDataDumpers(dumpTable);
        assertNotNull("dataDumpers can not be null", dataDumpers);

        assertEquals(1, dataDumpers.splitCount);
        Iterator<IDataSourceDumper> dumpers = dataDumpers.dumpers;
        Map<String, String> row = null;

        assertTrue("must contain a dumper", dumpers.hasNext());

        IDataSourceDumper dumper = dumpers.next();

        assertNotNull(dumper);

        assertEquals("jdbc:mysql://192.168.28.202:9030/", dumper.getDbHost());

       // dumper.closeResource();

    }

    public static DorisSourceFactory getDorisSourceFactory() {
        DorisSourceFactory dataSourceFactory = new DorisSourceFactory() {
//            @Override
//            protected Connection getConnection(String jdbcUrl, String username, String password) throws SQLException {
//                throw new UnsupportedOperationException();
//            }
        };

        dataSourceFactory.dbName = "employees";
        dataSourceFactory.password = "123456";
        dataSourceFactory.userName = "root";
        dataSourceFactory.nodeDesc = "192.168.28.202";
        dataSourceFactory.port = 9030;
        dataSourceFactory.encode = "utf8";
        dataSourceFactory.loadUrl = "[\"172.28.17.100:8030\", \"172.28.17.100:8030\"]";
        return dataSourceFactory;
    }


}
