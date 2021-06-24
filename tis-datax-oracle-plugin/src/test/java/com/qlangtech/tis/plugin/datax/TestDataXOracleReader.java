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
import com.qlangtech.tis.plugin.ds.oracle.OracleDataSourceFactory;
import com.qlangtech.tis.plugin.ds.oracle.TestOracleDataSourceFactory;
import junit.framework.TestCase;

import java.util.Optional;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-05-08 11:35
 **/
public class TestDataXOracleReader extends TestCase {
    public void testGetDftTemplate() {
        String dftTemplate = DataXOracleReader.getDftTemplate();
        assertNotNull("dftTemplate can not be null", dftTemplate);
    }

    public void testPluginExtraPropsLoad() throws Exception {
        Optional<PluginExtraProps> extraProps = PluginExtraProps.load(DataXOracleReader.class);
        assertTrue(extraProps.isPresent());
    }

    public void testDescGenerate() throws Exception {

        PluginDesc.testDescGenerate(DataXOracleReader.class, "oracle-datax-reader-descriptor.json");
    }

    public void testTemplateGenerate() throws Exception {

        final OracleDataSourceFactory dsFactory = TestOracleDataSourceFactory.createOracleDataSourceFactory();

        DataXOracleReader reader = new DataXOracleReader() {
            @Override
            public OracleDataSourceFactory getDataSourceFactory() {
                return dsFactory;
            }

            @Override
            public Class<?> getOwnerClass() {
                return DataXOracleReader.class;
            }
        };
        reader.splitPk = true;
        reader.fetchSize = 666;
        reader.template = DataXOracleReader.getDftTemplate();
        reader.session = "[\n" +
                "              \"alter session set NLS_DATE_FORMAT='yyyy-mm-dd hh24:mi:ss'\",\n" +
                "              \"alter session set NLS_TIMESTAMP_FORMAT='yyyy-mm-dd hh24:mi:ss'\",\n" +
                "              \"alter session set NLS_TIMESTAMP_TZ_FORMAT='yyyy-mm-dd hh24:mi:ss'\",\n" +
                "              \"alter session set TIME_ZONE='US/Pacific'\"\n" +
                "            ]";
        reader.dbName = "order1";
        reader.selectedTabs = TestSelectedTabs.createSelectedTabs(1);

        String dataXName = "dataXName";
        ReaderTemplate.validateDataXReader("oracle-datax-reader-template-assert.json", dataXName, reader);
    }
}
