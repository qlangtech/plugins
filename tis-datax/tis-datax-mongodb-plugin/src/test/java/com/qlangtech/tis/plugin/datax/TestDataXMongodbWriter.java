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
import com.qlangtech.tis.datax.ISelectedTab;
import com.qlangtech.tis.datax.impl.DataxReader;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.extension.util.PluginExtraProps;
import com.qlangtech.tis.plugin.common.WriterTemplate;
import com.qlangtech.tis.plugin.datax.test.TestSelectedTabs;
import com.qlangtech.tis.plugin.ds.mangodb.MangoDBDataSourceFactory;
import com.qlangtech.tis.trigger.util.JsonUtil;
import com.qlangtech.tis.util.DescriptorsJSON;
import junit.framework.TestCase;
import org.easymock.EasyMock;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-05-08 11:35
 **/
public class TestDataXMongodbWriter extends TestCase {
    public void testGetDftTemplate() {
        String dftTemplate = DataXMongodbWriter.getDftTemplate();
        assertNotNull("dftTemplate can not be null", dftTemplate);
    }

    public void testPluginExtraPropsLoad() throws Exception {
        Optional<PluginExtraProps> extraProps = PluginExtraProps.load(DataXMongodbWriter.class);
        assertTrue(extraProps.isPresent());
    }

    public void testDescriptorsJSONGenerate() {
        DataxReader dataxReader = EasyMock.createMock("dataxReader", DataxReader.class);

        List<ISelectedTab> selectedTabs = TestSelectedTabs.createSelectedTabs(1).stream().map((t) -> t).collect(Collectors.toList());

        for (ISelectedTab tab : selectedTabs) {
            for (ISelectedTab.ColMeta cm : tab.getCols()) {
                cm.setType(ISelectedTab.DataXReaderColType.STRING);
            }
        }
        EasyMock.expect(dataxReader.getSelectedTabs()).andReturn(selectedTabs).times(4);
        DataxReader.dataxReaderThreadLocal.set(dataxReader);
        EasyMock.replay(dataxReader);
        DataXMongodbWriter writer = new DataXMongodbWriter();
        DescriptorsJSON descJson = new DescriptorsJSON(writer.getDescriptor());

        JsonUtil.assertJSONEqual(DataXMongodbWriter.class, "mongdodb-datax-writer-descriptor.json"
                , descJson.getDescriptorsJSON(), (m, e, a) -> {
                    assertEquals(m, e, a);
                });

        JsonUtil.assertJSONEqual(DataXMongodbWriter.class, "mongdodb-datax-writer-descriptor.json"
                , descJson.getDescriptorsJSON(), (m, e, a) -> {
                    assertEquals(m, e, a);
                });
        EasyMock.verify(dataxReader);
    }

    public void testTemplateGenerate() throws Exception {


        MangoDBDataSourceFactory dsFactory = TestDataXMongodbReader.getDataSourceFactory();
        DataXMongodbWriter writer = new DataXMongodbWriter() {
            @Override
            public MangoDBDataSourceFactory getDsFactory() {
                return dsFactory;
            }

            @Override
            public Class<?> getOwnerClass() {
                return DataXMongodbWriter.class;
            }
        };
        writer.collectionName = "employee";

        writer.column = IOUtils.loadResourceFromClasspath(this.getClass(), "mongodb-reader-column.json");
        writer.template = DataXMongodbWriter.getDftTemplate();
        writer.dbName = "order1";
        writer.upsertInfo = "{\"isUpsert\":true,\"upsertKey\":\"user_id\"}";
       // IDataxProcessor.TableMap tableMap = new IDataxProcessor.TableMap();
        WriterTemplate.valiateCfgGenerate(
                "mongodb-datax-writer-assert.json", writer, null);

        dsFactory.username = null;
        dsFactory.password = null;
        writer.upsertInfo = null;

        WriterTemplate.valiateCfgGenerate(
                "mongodb-datax-writer-assert-without-option.json", writer, null);


    }
}
