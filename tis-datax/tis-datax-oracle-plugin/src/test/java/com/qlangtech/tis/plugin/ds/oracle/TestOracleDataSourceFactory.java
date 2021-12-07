/**
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
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
