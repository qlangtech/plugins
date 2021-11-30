/**
 * Copyright (c) 2020 QingLang, Inc. <baisui@qlangtech.com>
 * <p>
 *   This program is free software: you can use, redistribute, and/or modify
 *   it under the terms of the GNU Affero General Public License, version 3
 *   or later ("AGPL"), as published by the Free Software Foundation.
 * <p>
 *  This program is distributed in the hope that it will be useful, but WITHOUT
 *  ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 *   FITNESS FOR A PARTICULAR PURPOSE.
 * <p>
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.qlangtech.tis.plugin.datax.doris;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.impl.DataxProcessor;
import com.qlangtech.tis.datax.impl.DataxReader;
import com.qlangtech.tis.datax.impl.DataxWriter;
import com.qlangtech.tis.extension.util.PluginExtraProps;
import com.qlangtech.tis.manage.common.CenterResource;
import com.qlangtech.tis.manage.common.TisUTF8;
import com.qlangtech.tis.plugin.common.WriterTemplate;
import com.qlangtech.tis.plugin.datax.doris.DataXDorisWriter;
import com.qlangtech.tis.plugin.datax.test.TestSelectedTabs;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.ds.doris.DorisSourceFactory;
import com.qlangtech.tis.plugin.ds.doris.TestDorisSourceFactory;
import com.qlangtech.tis.trigger.util.JsonUtil;
import com.qlangtech.tis.util.DescriptorsJSON;
import junit.framework.TestCase;
import org.apache.commons.io.FileUtils;
import org.easymock.EasyMock;

import java.io.File;
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


    @Override
    public void setUp() throws Exception {
        super.setUp();
        CenterResource.setNotFetchFromCenterRepository();
    }

    private static final String DataXName = "test1dataXname";

    public void testGetDftTemplate() {
        String dftTemplate = DataXDorisWriter.getDftTemplate();
        assertNotNull("dftTemplate can not be null", dftTemplate);
    }

    public void testPluginExtraPropsLoad() throws Exception {
        Optional<PluginExtraProps> extraProps = PluginExtraProps.load(DataXDorisWriter.class);
        assertTrue(extraProps.isPresent());
        PluginExtraProps props = extraProps.get();
        PluginExtraProps.Props dbNameProp = props.getProp("dbName");
        assertNotNull(dbNameProp);
        JSONObject creator = dbNameProp.getProps().getJSONObject("creator");
        assertNotNull(creator);

    }

    public void testDescriptorsJSONGenerate() {
        DataxReader dataxReader = EasyMock.createMock("dataxReader", DataxReader.class);

        List<ISelectedTab> selectedTabs = TestSelectedTabs.createSelectedTabs(1).stream().map((t) -> t).collect(Collectors.toList());

        for (ISelectedTab tab : selectedTabs) {
            for (ISelectedTab.ColMeta cm : tab.getCols()) {
                cm.setType(ISelectedTab.DataXReaderColType.STRING.dataType);
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


        CreateDorisWriter createDorisWriter = new CreateDorisWriter().invoke();
        DorisSourceFactory dsFactory = createDorisWriter.getDsFactory();
        DataXDorisWriter writer = createDorisWriter.getWriter();

        // IDataxProcessor.TableMap tableMap = new IDataxProcessor.TableMap();

        IDataxProcessor.TableMap tableMap = new IDataxProcessor.TableMap();
        tableMap.setFrom("application");
        tableMap.setTo("application");
        List<ISelectedTab.ColMeta> sourceCols = Lists.newArrayList();
        ISelectedTab.ColMeta col = new ISelectedTab.ColMeta();
        col.setPk(true);
        col.setName("user_id");
        col.setType(ISelectedTab.DataXReaderColType.Long.dataType);
        sourceCols.add(col);

        col = new ISelectedTab.ColMeta();
        col.setName("user_name");
        col.setType(ISelectedTab.DataXReaderColType.STRING.dataType);
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

    public void testRealDump() throws Exception {

        String targetTableName = "customer_order_relation";
        String testDataXName = "mysql_doris";

        CreateDorisWriter createDorisWriter = new CreateDorisWriter().invoke();
        createDorisWriter.dsFactory.password = "";
        createDorisWriter.dsFactory.nodeDesc = "192.168.28.201";

        createDorisWriter.writer.autoCreateTable = true;

        DataxProcessor dataXProcessor = EasyMock.mock("dataXProcessor", DataxProcessor.class);
        File createDDLDir = new File(".");
        File createDDLFile = null;
        try {
            createDDLFile = new File(createDDLDir, targetTableName + IDataxProcessor.DATAX_CREATE_DDL_FILE_NAME_SUFFIX);
            FileUtils.write(createDDLFile
                    , com.qlangtech.tis.extension.impl.IOUtils.loadResourceFromClasspath(DataXDorisWriter.class, "create_ddl_customer_order_relation.sql"), TisUTF8.get());

            EasyMock.expect(dataXProcessor.getDataxCreateDDLDir(null)).andReturn(createDDLDir);
            DataxWriter.dataxWriterGetter = (dataXName) -> {
                return createDorisWriter.writer;
            };
            DataxProcessor.processorGetter = (dataXName) -> {
                assertEquals(testDataXName, dataXName);
                return dataXProcessor;
            };
            EasyMock.replay(dataXProcessor);
            //DataXDorisWriter writer = new DataXDorisWriter();
            WriterTemplate.realExecuteDump("doris_writer_real_dump.json", createDorisWriter.writer);

            EasyMock.verify(dataXProcessor);
        } finally {
            FileUtils.forceDelete(createDDLFile);
        }
    }

    private class CreateDorisWriter {
        private DorisSourceFactory dsFactory;
        private DataXDorisWriter writer;

        public DorisSourceFactory getDsFactory() {
            return dsFactory;
        }

        public DataXDorisWriter getWriter() {
            return writer;
        }

        public CreateDorisWriter invoke() {
            dsFactory = TestDorisSourceFactory.getDorisSourceFactory();
            writer = new DataXDorisWriter() {
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
            return this;
        }
    }
}
