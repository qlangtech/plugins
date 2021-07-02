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
import com.qlangtech.tis.plugin.ds.tidb.GetColsMeta;
import com.qlangtech.tis.plugin.ds.tidb.TestTiKVDataSourceFactory;
import com.qlangtech.tis.plugin.ds.tidb.TiKVDataSourceFactory;
import com.qlangtech.tis.plugin.test.BasicTest;

import java.util.Optional;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-06-05 20:48
 **/
public class TestDataXTiDBReader extends BasicTest {
    public void testGetDftTemplate() {
        String dftTemplate = DataXTiDBReader.getDftTemplate();
        assertNotNull("dftTemplate can not be null", dftTemplate);
    }

    public void testPluginExtraPropsLoad() throws Exception {
        Optional<PluginExtraProps> extraProps = PluginExtraProps.load(DataXTiDBReader.class);
        assertTrue(extraProps.isPresent());
    }

    public void testDescriptorsJSONGenerate() {
        PluginDesc.testDescGenerate(DataXTiDBReader.class, "tidb-datax-reader-descriptor.json");
    }

    public void testTemplateGenerate() throws Exception {

        final String dataXName = "dataXName";
        GetColsMeta getColsMeta = new GetColsMeta().invoke();
        final TiKVDataSourceFactory dsFactory = getColsMeta.getDataSourceFactory();
        DataXTiDBReader dataxReader = new DataXTiDBReader() {
            @Override
            public TiKVDataSourceFactory getDataSourceFactory() {
                return dsFactory;
            }

            @Override
            public Class<?> getOwnerClass() {
                return DataXTiDBReader.class;
            }
        };

        dataxReader.template = DataXTiDBReader.getDftTemplate();

        dataxReader.setSelectedTabs(TestTiKVDataSourceFactory.createTabOfEmployees());

        ReaderTemplate.validateDataXReader("tidb-datax-reader-template-assert.json", dataXName, dataxReader);
    }


    public void testRealDump() throws Exception {
        DataXTiDBReader dataxReader = new DataXTiDBReader();
        ReaderTemplate.realExecute("tidb-datax-reader-template-assert.json", dataxReader);
    }
}
