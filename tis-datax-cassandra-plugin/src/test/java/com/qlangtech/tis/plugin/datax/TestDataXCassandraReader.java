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

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.qlangtech.tis.extension.util.PluginExtraProps;
import com.qlangtech.tis.plugin.common.ReaderTemplate;
import com.qlangtech.tis.plugin.ds.cassandra.CassandraDatasourceFactory;
import com.qlangtech.tis.plugin.ds.cassandra.TestCassandraDatasourceFactory;
import com.qlangtech.tis.trigger.util.JsonUtil;
import com.qlangtech.tis.util.DescriptorsJSON;
import com.qlangtech.tis.util.UploadPluginMeta;
import junit.framework.TestCase;

import java.util.Collections;
import java.util.Optional;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-05-08 11:35
 **/
public class TestDataXCassandraReader extends TestCase {
    public void testGetDftTemplate() {
        String dftTemplate = DataXCassandraReader.getDftTemplate();
        assertNotNull("dftTemplate can not be null", dftTemplate);
    }

    public void testPluginExtraPropsLoad() throws Exception {
        Optional<PluginExtraProps> extraProps = PluginExtraProps.load(DataXCassandraReader.class);
        assertTrue(extraProps.isPresent());
    }

    public void testDescriptorsJSONGenerate() {
        DataXCassandraReader reader = new DataXCassandraReader();
        DescriptorsJSON descJson = new DescriptorsJSON(reader.getDescriptor());
        //System.out.println(descJson.getDescriptorsJSON().toJSONString());

        JsonUtil.assertJSONEqual(DataXCassandraReader.class, "cassandra-datax-reader-descriptor.json"
                , descJson.getDescriptorsJSON(), (m, e, a) -> {
                    assertEquals(m, e, a);
                });


        UploadPluginMeta pluginMeta
                = UploadPluginMeta.parse("dataxReader:require,targetDescriptorName_"+DataXCassandraReader.DATAX_NAME+",subFormFieldName_selectedTabs,dataxName_baisuitestTestcase");

        JSONObject subFormDescriptorsJSON = descJson.getDescriptorsJSON(pluginMeta.getSubFormFilter());

        JsonUtil.assertJSONEqual(DataXCassandraReader.class, "cassandra-datax-reader-selected-tabs-subform-descriptor.json"
                , subFormDescriptorsJSON, (m, e, a) -> {
                    assertEquals(m, e, a);
                });
    }


    public void testTemplateGenerate() throws Exception {

        CassandraDatasourceFactory dsFactory = TestCassandraDatasourceFactory.getDS();
        dsFactory.useSSL = true;
        String dataxName = "testDataX";
        String tabUserDtl = "user_dtl";
        DataXCassandraReader reader = new DataXCassandraReader() {
            @Override
            public CassandraDatasourceFactory getDataSourceFactory() {
                return dsFactory;
            }

            @Override
            public Class<?> getOwnerClass() {
                return DataXCassandraReader.class;
            }
        };
        reader.template = DataXCassandraReader.getDftTemplate();
        reader.allowFiltering = true;
        reader.consistancyLevel = "QUORUM";


        SelectedTab selectedTab = new SelectedTab();
        selectedTab.setCols(Lists.newArrayList("col2", "col1", "col3"));
        selectedTab.setWhere("delete = 0");
        selectedTab.name = tabUserDtl;
        reader.setSelectedTabs(Collections.singletonList(selectedTab));

        ReaderTemplate.validateDataXReader("cassandra-datax-reader-assert.json", dataxName, reader);

        dsFactory.useSSL = null;
        reader.allowFiltering = null;
        reader.consistancyLevel = null;
        selectedTab.setWhere(null);
        ReaderTemplate.validateDataXReader("cassandra-datax-reader-assert-without-option.json", dataxName, reader);
    }
}
