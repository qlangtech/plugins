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
import com.qlangtech.tis.plugin.common.ReaderTemplate;
import com.qlangtech.tis.plugin.datax.test.TestSelectedTabs;
import com.qlangtech.tis.plugin.ds.postgresql.PGDataSourceFactory;
import com.qlangtech.tis.trigger.util.JsonUtil;
import com.qlangtech.tis.util.DescriptorsJSON;
import junit.framework.TestCase;

import java.util.Optional;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-05-08 11:35
 **/
public class TestDataXPostgresqlReader extends TestCase {
    public void testGetDftTemplate() {
        String dftTemplate = DataXPostgresqlReader.getDftTemplate();
        assertNotNull("dftTemplate can not be null", dftTemplate);
    }

    public void testPluginExtraPropsLoad() throws Exception {
        Optional<PluginExtraProps> extraProps = PluginExtraProps.load(DataXPostgresqlReader.class);
        assertTrue(extraProps.isPresent());
    }

    public void testDescriptorsJSONGenerate() {
        DataXPostgresqlReader esWriter = new DataXPostgresqlReader();
        DescriptorsJSON descJson = new DescriptorsJSON(esWriter.getDescriptor());

        JsonUtil.assertJSONEqual(DataXPostgresqlReader.class
                , "postgres-datax-reader-descriptor.json"
                , descJson.getDescriptorsJSON(), (m, e, a) -> {
                    assertEquals(m, e, a);
                });
    }

    public void testTemplateGenerate() throws Exception {

        PGDataSourceFactory dsFactory = new PGDataSourceFactory();
        dsFactory.dbName = "order1";
        dsFactory.password = "123455*^";
        dsFactory.userName = "admin";
        dsFactory.port = 5432;
        dsFactory.encode = "utf8";
        dsFactory.extraParams = "aa=bb&cc=xxx";
        dsFactory.nodeDesc = "192.168.28.201";

        DataXPostgresqlReader reader = new DataXPostgresqlReader() {
            @Override
            public PGDataSourceFactory getDataSourceFactory() {
                return dsFactory;
            }

            @Override
            public Class<?> getOwnerClass() {
                return DataXPostgresqlReader.class;
            }
        };
        reader.setSelectedTabs(TestSelectedTabs.createSelectedTabs(1));
        reader.fetchSize = 333;
        reader.splitPk = true;
        reader.template = DataXPostgresqlReader.getDftTemplate();

        String dataXName = "dataxName";

        ReaderTemplate.validateDataXReader("postgres-datax-reader-assert.json", dataXName, reader);


        reader.fetchSize = null;
        reader.splitPk = null;

        ReaderTemplate.validateDataXReader("postgres-datax-reader-assert-without-option.json", dataXName, reader);

    }
}
