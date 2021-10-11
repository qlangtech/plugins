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

package com.qlangtech.tis.plugin.ds.oracle;

import com.qlangtech.tis.TIS;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.ExtensionList;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.plugin.common.PluginDesc;
import com.qlangtech.tis.plugin.ds.ColumnMetaData;
import com.qlangtech.tis.plugin.ds.DataSourceFactory;
import junit.framework.TestCase;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.Optional;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-06-24 14:11
 **/
public class TestOracleDataSourceFactory extends TestCase {
    public void testDescGenerate() {
        PluginDesc.testDescGenerate(OracleDataSourceFactory.class, "oracle-datax-ds-factory-descriptor.json");
    }

    public void testDSDescPresent() {
        ExtensionList<Descriptor<DataSourceFactory>> descriptorList = TIS.get().getDescriptorList(DataSourceFactory.class);
        Optional<Descriptor<DataSourceFactory>> match = descriptorList.stream().filter((desc) -> OracleDataSourceFactory.ORACLE.equals(desc.getDisplayName())).findFirst();
        assertTrue(OracleDataSourceFactory.ORACLE + " desc must present", match.isPresent());
    }

    public void testShowTableInDB() {

        // System.out.println("SELECT NULL AS table_cat,\n       c.owner AS table_schem,\n       c.table_name,\n       c.column_name,\n       c.position AS key_seq,\n       c.constraint_name AS pk_name\nFROM all_cons_columns c, all_constraints k\nWHERE k.constraint_type = 'P'\n  AND k.table_name = :1\n  AND k.owner like :2 escape '/'\n  AND k.constraint_name = c.constraint_name \n  AND k.table_name = c.table_name \n  AND k.owner = c.owner \nORDER BY column_name\n");

        String createDDL = IOUtils.loadResourceFromClasspath(TestOracleDataSourceFactory.class, "create-sql-instancedetail.sql");
        System.out.println(createDDL);

        OracleDataSourceFactory dsFactory = createOracleDataSourceFactory();

        List<String> tablesInDB = dsFactory.getTablesInDB();
        assertTrue(tablesInDB.size() > 1);
       // tablesInDB.forEach((tab) -> System.out.println(tab));
        List<ColumnMetaData> cols = dsFactory.getTableMetadata(StringUtils.upperCase("instancedetail"));

        assertTrue(cols.size() > 0);

        for (ColumnMetaData col : cols) {
            System.out.println(col.getKey() + " " + col.isPk() + " " + col.getType());
        }

    }

    public static OracleDataSourceFactory createOracleDataSourceFactory() {
        OracleDataSourceFactory dsFactory = new OracleDataSourceFactory();
        dsFactory.dbName = "xe";
        dsFactory.userName = "system";
        dsFactory.password = "oracle";
        dsFactory.nodeDesc = "192.168.28.201";
        dsFactory.port = 1521;
        return dsFactory;
    }
}
