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
import com.qlangtech.tis.plugin.ds.oracle.OracleDataSourceFactory;
import com.qlangtech.tis.plugin.ds.oracle.TestOracleDataSourceFactory;
import junit.framework.TestCase;

import java.util.Optional;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-05-08 11:35
 **/
public class TestDataXOracleWriter extends TestCase {
    public void testGetDftTemplate() {
        String dftTemplate = DataXOracleWriter.getDftTemplate();
        assertNotNull("dftTemplate can not be null", dftTemplate);
    }

    public void testPluginExtraPropsLoad() throws Exception {
        Optional<PluginExtraProps> extraProps = PluginExtraProps.load(DataXOracleWriter.class);
        assertTrue(extraProps.isPresent());
    }

    public void testDescGenerate() throws Exception {

        PluginDesc.testDescGenerate(DataXOracleWriter.class, "oracle-datax-writer-descriptor.json");
    }

    public void testTemplateGenerate() throws Exception {
        OracleDataSourceFactory dsFactory = TestOracleDataSourceFactory.createOracleDataSourceFactory();

        DataXOracleWriter writer = new DataXOracleWriter() {
            @Override
            protected OracleDataSourceFactory getDataSourceFactory() {
                return dsFactory;
            }

            @Override
            public Class<?> getOwnerClass() {
                return DataXOracleWriter.class;
            }
        };
        writer.template = DataXOracleWriter.getDftTemplate();
        writer.batchSize = 1235;
        writer.postSql = "drop table @table";
        writer.preSql = "drop table @table";
        writer.dbName = "testdb";
        writer.session ="[\n" +
                "            \"alter session set nls_date_format = 'dd.mm.yyyy hh24:mi:ss';\"\n" +
                "            \"alter session set NLS_LANG = 'AMERICAN';\"\n" +
                "]";

        Optional<IDataxProcessor.TableMap> tableMap = TestSelectedTabs.createTableMapper();



        WriterTemplate.valiateCfgGenerate("oracle-datax-writer-assert.json", writer, tableMap.get());

        writer.session = null;
        writer.preSql = null;
        writer.postSql = null;
        writer.batchSize = null;

        WriterTemplate.valiateCfgGenerate("oracle-datax-writer-assert-without-option.json", writer, tableMap.get());
    }
}
