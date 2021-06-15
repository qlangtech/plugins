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

import com.google.common.collect.Lists;
import com.qlangtech.tis.config.aliyun.IAliyunToken;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.ISelectedTab;
import com.qlangtech.tis.datax.impl.DataxReader;
import com.qlangtech.tis.datax.impl.DataxWriter;
import com.qlangtech.tis.extension.util.PluginExtraProps;
import com.qlangtech.tis.plugin.common.WriterTemplate;
import com.qlangtech.tis.plugin.test.BasicTest;
import com.qlangtech.tis.trigger.util.JsonUtil;
import com.qlangtech.tis.util.DescriptorsJSON;
import org.easymock.EasyMock;

import java.util.List;
import java.util.Optional;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-05-08 11:35
 **/
public class TestDataXElasticsearchWriter extends BasicTest {

    public void testDescriptorsJSONGenerate() {

        DataxReader dataxReader = EasyMock.createMock("dataxReader", DataxReader.class);
        List<ISelectedTab> selectedTabs = createSelectedTabs(dataxReader);
        EasyMock.expect(dataxReader.getSelectedTabs()).andReturn(selectedTabs);


        EasyMock.replay(dataxReader);
        DataXElasticsearchWriter esWriter = new DataXElasticsearchWriter();
        DescriptorsJSON descJson = new DescriptorsJSON(esWriter.getDescriptor());
//        descJson.getDescriptorsJSON().toJSONString();

        JsonUtil.assertJSONEqual(DataXElasticsearchWriter.class, "es-datax-writer-descriptor.json"
                , descJson.getDescriptorsJSON(), (m, e, a) -> {
                    assertEquals(m, e, a);
                });


        EasyMock.verify(dataxReader);
    }

    protected List<ISelectedTab> createSelectedTabs(DataxReader dataxReader) {
        List<ISelectedTab> selectedTabs = Lists.newArrayList();
        SelectedTab tab = new SelectedTab();
        List<String> cols = Lists.newArrayList();
        cols.add("app_id");
        cols.add("project_name");
        tab.setCols(cols);
        List<ISelectedTab.ColMeta> cols1 = tab.getCols();
        cols1.get(0).setType(ISelectedTab.DataXReaderColType.INT);
        cols1.get(1).setType(ISelectedTab.DataXReaderColType.STRING);
        selectedTabs.add(tab);
        DataxWriter.dataReaderThreadlocal.set(dataxReader);
        return selectedTabs;
    }

    public void testGetDftTemplate() {
        String dftTemplate = DataXElasticsearchWriter.getDftTemplate();
        assertNotNull("dftTemplate can not be null", dftTemplate);
    }

    public void testPluginExtraPropsLoad() throws Exception {
        Optional<PluginExtraProps> extraProps = PluginExtraProps.load(DataXElasticsearchWriter.class);
        assertTrue(extraProps.isPresent());
    }

    public void testTemplateGenerate() throws Exception {

        DataxReader dataxReader = EasyMock.createMock("dataxReader", DataxReader.class);
        List<ISelectedTab> selectedTabs = createSelectedTabs(dataxReader);
        EasyMock.expect(dataxReader.getSelectedTabs()).andReturn(selectedTabs);

        final TestAliyunToken token = new TestAliyunToken("xxxxxxxxxxx", "accessKeykkkkkkkkkkkkkk");

        DataXElasticsearchWriter dataXWriter = new DataXElasticsearchWriter() {
            @Override
            public IAliyunToken getToken() {
                return token;
            }

            @Override
            public String getTemplate() {
                return DataXElasticsearchWriter.getDftTemplate();
            }

            @Override
            public Class<?> getOwnerClass() {
                return DataXElasticsearchWriter.class;
            }
        };
        EasyMock.replay(dataxReader);
        dataXWriter.endpoint = "aliyun-bj-endpoint";
        dataXWriter.column = DataXElasticsearchWriter.getDftColumn();
        dataXWriter.alias = "application2";
        dataXWriter.index = "application";
        dataXWriter.type = "specific_type";
        dataXWriter.cleanup = true;
        dataXWriter.batchSize = 9999;
        dataXWriter.trySize = 22;
        dataXWriter.timeout = 6666;
        dataXWriter.discovery = true;
        dataXWriter.compression = true;
        dataXWriter.multiThread = true;
        dataXWriter.ignoreParseError = true;
        dataXWriter.aliasMode = "append";
        dataXWriter.settings = "{\"index\" :{\"number_of_shards\": 1, \"number_of_replicas\": 0}}";
        dataXWriter.splitter = ",";
        dataXWriter.dynamic = true;


        IDataxProcessor.TableMap tableMap = new IDataxProcessor.TableMap();


        WriterTemplate.valiateCfgGenerate("es-datax-writer-assert.json", dataXWriter, tableMap);


        token.accessKeyId = null;
        token.sccessKeySecret = null;

        WriterTemplate.valiateCfgGenerate("es-datax-writer-assert-without-option.json", dataXWriter, tableMap);


        EasyMock.verify(dataxReader);
    }

//    public void testRealDump() throws Exception {
//
//        DataXElasticsearchWriter dataXWriter = new DataXElasticsearchWriter();
//
//        WriterTemplate.realExecuteDump("es-datax-writer-real-dump.json", dataXWriter);
//    }


    private static class TestAliyunToken implements IAliyunToken {
        private String accessKeyId;
        private String sccessKeySecret;

        public TestAliyunToken(String accessKeyId, String sccessKeySecret) {
            this.accessKeyId = accessKeyId;
            this.sccessKeySecret = sccessKeySecret;
        }

        @Override
        public String getAccessKeyId() {
            return accessKeyId;
        }

        @Override
        public String getAccessKeySecret() {
            return sccessKeySecret;
        }

        @Override
        public String getEndpoint() {
            return "http://oss-cn-beijing.aliyuncs.com";
        }
    }
}
