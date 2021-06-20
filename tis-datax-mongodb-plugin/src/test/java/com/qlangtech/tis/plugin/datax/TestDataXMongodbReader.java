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

import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.extension.util.PluginExtraProps;
import com.qlangtech.tis.plugin.common.ReaderTemplate;
import com.qlangtech.tis.plugin.ds.mangodb.MangoDBDataSourceFactory;
import com.qlangtech.tis.trigger.util.JsonUtil;
import com.qlangtech.tis.util.DescriptorsJSON;
import junit.framework.TestCase;

import java.util.Optional;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-05-08 11:35
 **/
public class TestDataXMongodbReader extends TestCase {
    public void testGetDftTemplate() {
        String dftTemplate = DataXMongodbReader.getDftTemplate();
        assertNotNull("dftTemplate can not be null", dftTemplate);
    }

    public void testPluginExtraPropsLoad() throws Exception {
        Optional<PluginExtraProps> extraProps = PluginExtraProps.load(DataXMongodbReader.class);
        assertTrue(extraProps.isPresent());
    }

    public void testDescriptorsJSONGenerate() {
        DataXMongodbReader reader = new DataXMongodbReader();
        DescriptorsJSON descJson = new DescriptorsJSON(reader.getDescriptor());

        JsonUtil.assertJSONEqual(DataXMongodbReader.class, "mongdodb-datax-reader-descriptor.json"
                , descJson.getDescriptorsJSON(), (m, e, a) -> {
                    assertEquals(m, e, a);
                });

    }

    public void testTemplateGenerate() throws Exception {
        String dataXName = "testDataXName";
        MangoDBDataSourceFactory dsFactory = getDataSourceFactory();


        DataXMongodbReader reader = new DataXMongodbReader() {
            @Override
            public MangoDBDataSourceFactory getDsFactory() {
                return dsFactory;
            }

            @Override
            public Class<?> getOwnerClass() {
                return DataXMongodbReader.class;
            }
        };
        reader.column = IOUtils.loadResourceFromClasspath(this.getClass(), "mongodb-reader-column.json");
        reader.query = "this is my query";
        reader.collectionName = "employee";
        reader.template = DataXMongodbReader.getDftTemplate();


        ReaderTemplate.validateDataXReader("mongodb-datax-reader-assert.json", dataXName, reader);

        reader.query = null;
        dsFactory.password = null;
        ReaderTemplate.validateDataXReader("mongodb-datax-reader-assert-without-option.json", dataXName, reader);
    }

    public static MangoDBDataSourceFactory getDataSourceFactory() {
        MangoDBDataSourceFactory dsFactory = new MangoDBDataSourceFactory();
        dsFactory.dbName = "order1";
        dsFactory.address = "192.168.28.200:27017;192.168.28.201:27017";
        dsFactory.password = "123456";
        dsFactory.username = "root";
        return dsFactory;
    }


}
