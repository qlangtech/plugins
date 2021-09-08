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

package com.qlangtech.tis.plugin.datax;

import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.extension.util.PluginExtraProps;
import com.qlangtech.tis.plugin.common.PluginDesc;
import com.qlangtech.tis.plugin.common.WriterTemplate;
import com.qlangtech.tis.plugin.datax.test.TestSelectedTabs;
import com.qlangtech.tis.plugin.ds.sqlserver.SqlServerDatasourceFactory;
import junit.framework.TestCase;

import java.util.Optional;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-05-08 11:35
 **/
public class TestDataXSqlserverWriter extends TestCase {
    public void testGetDftTemplate() {
        String dftTemplate = DataXSqlserverWriter.getDftTemplate();
        assertNotNull("dftTemplate can not be null", dftTemplate);
    }

    public void testPluginExtraPropsLoad() throws Exception {
        Optional<PluginExtraProps> extraProps = PluginExtraProps.load(DataXSqlserverWriter.class);
        assertTrue(extraProps.isPresent());
    }


    public void testDescGenerate() {
        PluginDesc.testDescGenerate(DataXSqlserverWriter.class, "sqlserver-datax-writer-descriptor.json");
    }

    public void testTemplateGenerate() throws Exception {


        SqlServerDatasourceFactory dsFactory = getSqlServerDSFactory();

        DataXSqlserverWriter writer = new DataXSqlserverWriter() {
//            @Override
//            protected SqlServerDatasourceFactory getDataSourceFactory() {
//                return dsFactory;
//            }

            @Override
            public SqlServerDatasourceFactory getDataSourceFactory() {
                return dsFactory;
            }

            @Override
            public Class<?> getOwnerClass() {
                return DataXSqlserverWriter.class;
            }
        };
        writer.template = DataXSqlserverWriter.getDftTemplate();
        writer.batchSize = 1234;
        writer.postSql = "drop table @table";
        writer.preSql = "drop table @table";
        writer.dbName = "testdb";

        Optional<IDataxProcessor.TableMap> tableMap = TestSelectedTabs.createTableMapper();

        WriterTemplate.valiateCfgGenerate("sqlserver-datax-writer-assert.json", writer, tableMap.get());
    }

    public static SqlServerDatasourceFactory getSqlServerDSFactory() {
        SqlServerDatasourceFactory dsFactory = new SqlServerDatasourceFactory();
        dsFactory.dbName = "tis";
        dsFactory.password = "Hello1234!";
        dsFactory.userName = "sa";
        dsFactory.port = 1433;
        dsFactory.nodeDesc = "192.168.28.201";
        return dsFactory;
    }
}
