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

package com.qlangtech.tis.plugin.ds.tidb;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.qlangtech.tis.plugin.datax.SelectedTab;
import com.qlangtech.tis.plugin.ds.ColumnMetaData;
import com.qlangtech.tis.plugin.ds.DataDumpers;
import com.qlangtech.tis.plugin.ds.IDataSourceDumper;
import com.qlangtech.tis.plugin.ds.TISTable;
import junit.framework.TestCase;

import java.sql.Types;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author: baisui 百岁
 * @create: 2020-11-24 17:57
 **/
public class TestTiKVDataSourceFactory extends TestCase {
    public static final String DB_NAME = "employees";
    public static final String TABLE_NAME = "employees";

    private static final String COL_EMP_NO = "emp_no";
    private static final String COL_BIRTH_DATE = "birth_date";
    private static final String COL_FIRST_NAME = "first_name";
    private static final String COL_LAST_NAME = "last_name";
    private static final String COL_GENDER = "gender";
    private static final String COL_HIRE_DATE = "hire_date";

    public void testColMetadata() {

        validateColumnMeta(true);
        validateColumnMeta(false);
    }

    public static List<SelectedTab> createTabOfEmployees() {
        List<SelectedTab> selectedTabs = Lists.newArrayList();
        SelectedTab selectedTab = new SelectedTab();
        selectedTab.setCols(Lists.newArrayList(COL_EMP_NO, COL_BIRTH_DATE, COL_FIRST_NAME, COL_LAST_NAME, COL_GENDER, COL_HIRE_DATE));
        selectedTab.name = TABLE_NAME;
        selectedTabs.add(selectedTab);
        return selectedTabs;
    }

    private void validateColumnMeta(boolean datetimeFormat) {
        GetColsMeta getColsMeta = new GetColsMeta().invoke(datetimeFormat);

        List<ColumnMetaData> employeesCols = getColsMeta.getEmployeesCols();
        assertNotNull(employeesCols);
        assertEquals(6, employeesCols.size());
        ColumnMetaData pk = null;
        Map<String, Integer> colTypes = Maps.newHashMap();
        colTypes.put(COL_EMP_NO, Types.BIGINT);
        colTypes.put(COL_BIRTH_DATE, datetimeFormat ? Types.DATE : Types.INTEGER);
        colTypes.put(COL_FIRST_NAME, Types.VARCHAR);
        colTypes.put(COL_LAST_NAME, Types.VARCHAR);
        colTypes.put(COL_GENDER, Types.VARCHAR);
        colTypes.put(COL_HIRE_DATE, datetimeFormat ? Types.DATE : Types.INTEGER);
        Integer colType = null;
        for (ColumnMetaData cmeta : employeesCols) {
            if (cmeta.isPk()) {
                pk = cmeta;
            }
            colType = colTypes.get(cmeta.getKey());
            assertNotNull(colType);
            assertEquals(cmeta.getKey(), (int) colType, cmeta.getType());

            System.out.println(cmeta.getIndex() + ":" + cmeta.getKey() + ":" + cmeta.getType());
        }
        assertNotNull(pk);
        assertEquals("emp_no", pk.getKey());
    }

    public void testGetPlugin() {

        GetColsMeta getColsMeta = new GetColsMeta().invoke();
        TiKVDataSourceFactory dataSourceFactory = getColsMeta.getDataSourceFactory();
        List<ColumnMetaData> employeesCols = getColsMeta.getEmployeesCols();


        TISTable dumpTable = new TISTable();
        dumpTable.setDbName(DB_NAME);
        dumpTable.setTableName(TABLE_NAME);
        dumpTable.setReflectCols(employeesCols);

        DataDumpers dataDumpers = dataSourceFactory.getDataDumpers(dumpTable);

        assertEquals(1, dataDumpers.splitCount);

        Iterator<IDataSourceDumper> dumpers = dataDumpers.dumpers;
        Map<String, String> row = null;
        StringBuffer rowContent = null;
        int rowCount = 0;
        while (dumpers.hasNext()) {
            IDataSourceDumper dumper = dumpers.next();
//            assertEquals(300024, );
            assertTrue(dumper.getRowSize() > 0);
            try {
                Iterator<Map<String, String>> rowIterator = dumper.startDump();

                while (rowIterator.hasNext()) {
                    rowContent = new StringBuffer();
                    row = rowIterator.next();
//                    if ("251149".equals(row.get("emp_no"))) {
//                        System.out.println("==========="+row.get("emp_no"));
//                    }
                    for (Map.Entry<String, String> entry : row.entrySet()) {
                        rowContent.append(entry.getKey()).append(":").append(entry.getValue()).append(",");
                    }
                    rowCount++;
                    System.out.println(rowContent);
                }
            } finally {
                dumper.closeResource();
            }
        }
        assertEquals(300024, rowCount);
    }

}
