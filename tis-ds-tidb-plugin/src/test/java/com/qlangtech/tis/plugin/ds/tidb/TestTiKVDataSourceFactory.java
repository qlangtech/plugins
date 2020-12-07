package com.qlangtech.tis.plugin.ds.tidb;

import com.qlangtech.tis.plugin.ds.ColumnMetaData;
import com.qlangtech.tis.plugin.ds.DataDumpers;
import com.qlangtech.tis.plugin.ds.IDataSourceDumper;
import com.qlangtech.tis.plugin.ds.TISTable;
import junit.framework.TestCase;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author: baisui 百岁
 * @create: 2020-11-24 17:57
 **/
public class TestTiKVDataSourceFactory extends TestCase {
    private static final String DB_NAME = "employees";
    private static final String TABLE_NAME = "employees";

    public void testGetPlugin() {

        com.qlangtech.tis.plugin.ds.tidb.TiKVDataSourceFactory dataSourceFactory = new com.qlangtech.tis.plugin.ds.tidb.TiKVDataSourceFactory();
        dataSourceFactory.dbName = DB_NAME;
        dataSourceFactory.pdAddrs = "192.168.28.201:44552";
        List<ColumnMetaData> employeesCols = dataSourceFactory.getTableMetadata(TABLE_NAME);
        assertNotNull(employeesCols);
        assertEquals(6, employeesCols.size());
        for (ColumnMetaData cmeta : employeesCols) {
            System.out.println(cmeta.getIndex() + ":" + cmeta.getKey() + ":" + cmeta.getType());
        }

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

//        List<Descriptor<DataSourceFactory>> descList
//                = TIS.get().getDescriptorList(DataSourceFactory.class);
//        assertNotNull(descList);
//        assertEquals(1, descList.size());


//        Descriptor<DataSourceFactory> mysqlDS = descList.get(0);
//
//        mysqlDS.validate()
    }
}
