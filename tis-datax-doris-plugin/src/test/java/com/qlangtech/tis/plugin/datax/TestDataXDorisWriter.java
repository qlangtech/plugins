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
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.ISelectedTab;
import com.qlangtech.tis.datax.impl.DataxReader;
import com.qlangtech.tis.extension.util.PluginExtraProps;
import com.qlangtech.tis.plugin.common.WriterTemplate;
import com.qlangtech.tis.plugin.datax.test.TestSelectedTabs;
import com.qlangtech.tis.plugin.ds.doris.DorisSourceFactory;
import com.qlangtech.tis.plugin.ds.doris.TestDorisSourceFactory;
import com.qlangtech.tis.trigger.util.JsonUtil;
import com.qlangtech.tis.util.DescriptorsJSON;
import junit.framework.TestCase;
import org.easymock.EasyMock;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-09-07 12:04
 **/
public class TestDataXDorisWriter extends TestCase {
//    public void testGenDesc() {
//        ContextDesc.descBuild(DataXDorisWriter.class, false);
//    }


    private static final String DataXName = "test1dataXname";

    public void testGetDftTemplate() {
        String dftTemplate = DataXDorisWriter.getDftTemplate();
        assertNotNull("dftTemplate can not be null", dftTemplate);
    }

    public void testPluginExtraPropsLoad() throws Exception {
        Optional<PluginExtraProps> extraProps = PluginExtraProps.load(DataXDorisWriter.class);
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
      //  EasyMock.expect(dataxReader.getSelectedTabs()).andReturn(selectedTabs).anyTimes();
        DataxReader.dataxReaderThreadLocal.set(dataxReader);
        EasyMock.replay(dataxReader);
        DataXDorisWriter writer = new DataXDorisWriter();
        DescriptorsJSON descJson = new DescriptorsJSON(writer.getDescriptor());

        JsonUtil.assertJSONEqual(DataXDorisWriter.class, "doris-datax-writer-descriptor.json"
                , descJson.getDescriptorsJSON(), (m, e, a) -> {
                    assertEquals(m, e, a);
                });

        JsonUtil.assertJSONEqual(DataXDorisWriter.class, "doris-datax-writer-descriptor.json"
                , descJson.getDescriptorsJSON(), (m, e, a) -> {
                    assertEquals(m, e, a);
                });
        EasyMock.verify(dataxReader);
    }

    public void testTemplateGenerate() throws Exception {


        DorisSourceFactory dsFactory = TestDorisSourceFactory.getDorisSourceFactory();
        DataXDorisWriter writer = new DataXDorisWriter() {
            @Override
            public DorisSourceFactory getDataSourceFactory() {
                return dsFactory;
            }

            @Override
            public Class<?> getOwnerClass() {
                return DataXDorisWriter.class;
            }
        };
        writer.dataXName = DataXName;// .collectionName = "employee";

        //writer..column = IOUtils.loadResourceFromClasspath(this.getClass(), "mongodb-reader-column.json");
        writer.template = DataXDorisWriter.getDftTemplate();
        writer.dbName = "order1";
        writer.preSql = "drop table @table";
        writer.postSql = "drop table @table";
        writer.loadProps = "{\n" +
                "    \"column_separator\": \"\\\\x01\",\n" +
                "    \"row_delimiter\": \"\\\\x02\"\n" +
                "}";

        writer.maxBatchRows = 999;
        writer.batchSize = 1001;

        // IDataxProcessor.TableMap tableMap = new IDataxProcessor.TableMap();

        IDataxProcessor.TableMap tableMap = new IDataxProcessor.TableMap();
        tableMap.setFrom("application");
        tableMap.setTo("application");
        List<ISelectedTab.ColMeta> sourceCols = Lists.newArrayList();
        ISelectedTab.ColMeta col = new ISelectedTab.ColMeta();
        col.setPk(true);
        col.setName("user_id");
        col.setType(ISelectedTab.DataXReaderColType.Long);
        sourceCols.add(col);

        col = new ISelectedTab.ColMeta();
        col.setName("user_name");
        col.setType(ISelectedTab.DataXReaderColType.STRING);
        sourceCols.add(col);

        tableMap.setSourceCols(sourceCols);

        WriterTemplate.valiateCfgGenerate(
                "doris-datax-writer-assert.json", writer, tableMap);

        dsFactory.password = null;
        writer.preSql = null;
        writer.postSql = null;
        writer.loadProps = null;

        writer.maxBatchRows = null;
        writer.batchSize = null;

        WriterTemplate.valiateCfgGenerate(
                "doris-datax-writer-assert-without-optional.json", writer, tableMap);


    }
}
