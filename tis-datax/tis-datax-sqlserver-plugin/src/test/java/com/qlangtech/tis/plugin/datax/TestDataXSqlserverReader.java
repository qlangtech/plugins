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

import com.qlangtech.tis.extension.util.PluginExtraProps;
import com.qlangtech.tis.plugin.common.PluginDesc;
import com.qlangtech.tis.plugin.common.ReaderTemplate;
import com.qlangtech.tis.plugin.datax.test.TestSelectedTabs;
import com.qlangtech.tis.plugin.ds.sqlserver.SqlServerDatasourceFactory;
import junit.framework.TestCase;

import java.util.Optional;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-05-08 11:35
 **/
public class TestDataXSqlserverReader extends TestCase {
    public void testGetDftTemplate() {
        String dftTemplate = DataXSqlserverReader.getDftTemplate();
        assertNotNull("dftTemplate can not be null", dftTemplate);
    }

    public void testPluginExtraPropsLoad() throws Exception {
        Optional<PluginExtraProps> extraProps = PluginExtraProps.load(DataXSqlserverReader.class);
        assertTrue(extraProps.isPresent());
    }

    public void testDescGenerate() {

      //   com.qlangtech.tis.plugin.common.ContextDesc.descBuild(DataXSqlserverReader.class);

        PluginDesc.testDescGenerate(DataXSqlserverReader.class, "sqlserver-datax-reader-descriptor.json");
    }

    public void testTemplateGenerate() throws Exception {
        SqlServerDatasourceFactory dsFactory = TestDataXSqlserverWriter.getSqlServerDSFactory();
        DataXSqlserverReader reader = new DataXSqlserverReader() {
            @Override
            public SqlServerDatasourceFactory getDataSourceFactory() {
                return dsFactory;
            }

            @Override
            public Class<?> getOwnerClass() {
                return DataXSqlserverReader.class;
            }
        };
        reader.splitPk = true;
        reader.fetchSize = 1024;
        reader.selectedTabs = TestSelectedTabs.createSelectedTabs(1);
        reader.template = DataXSqlserverReader.getDftTemplate();
        String dataXName = "dataXName";
        ReaderTemplate.validateDataXReader("sqlserver-datax-reader-assert.json", dataXName, reader);
    }

}
