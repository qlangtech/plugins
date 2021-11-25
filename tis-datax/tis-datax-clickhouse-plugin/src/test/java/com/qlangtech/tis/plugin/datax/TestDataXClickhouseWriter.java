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
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.datax.impl.DataxProcessor;
import com.qlangtech.tis.datax.impl.DataxWriter;
import com.qlangtech.tis.extension.util.PluginExtraProps;
import com.qlangtech.tis.manage.common.TisUTF8;
import com.qlangtech.tis.plugin.common.PluginDesc;
import com.qlangtech.tis.plugin.common.WriterTemplate;
import com.qlangtech.tis.plugin.ds.clickhouse.ClickHouseDataSourceFactory;
import org.apache.commons.io.FileUtils;
import org.apache.curator.shaded.com.google.common.collect.Lists;
import org.easymock.EasyMock;

import java.io.File;
import java.util.List;
import java.util.Optional;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-05-08 11:35
 **/
public class TestDataXClickhouseWriter extends com.qlangtech.tis.plugin.test.BasicTest {
    public void testGetDftTemplate() {
        String dftTemplate = DataXClickhouseWriter.getDftTemplate();
        assertNotNull("dftTemplate can not be null", dftTemplate);
    }

    public void testPluginExtraPropsLoad() throws Exception {
        Optional<PluginExtraProps> extraProps = PluginExtraProps.load(DataXClickhouseWriter.class);
        assertTrue(extraProps.isPresent());
        assertEquals(9, extraProps.get().size());
    }

    private static final String testDataXName = "clickhouseWriter";
    private static final String clickhouse_datax_writer_assert_without_optional = "clickhouse-datax-writer-assert-without-optional.json";
    private static final String targetTableName = "customer_order_relation";

    public void testDescriptorsJSONGenerate() {
//        DataXClickhouseWriter writer = new DataXClickhouseWriter();
//        DescriptorsJSON descJson = new DescriptorsJSON(writer.getDescriptor());
//        //System.out.println(descJson.getDescriptorsJSON().toJSONString());
//
//        JsonUtil.assertJSONEqual(DataXClickhouseWriter.class, "clickhouse-datax-writer-descriptor.json"
//                , descJson.getDescriptorsJSON(), (m, e, a) -> {
//                    assertEquals(m, e, a);
//                });

        PluginDesc.testDescGenerate(DataXClickhouseWriter.class, "clickhouse-datax-writer-descriptor.json");
    }

    public void testConfigGenerate() throws Exception {
//        String dbName = "tis";
//        ClickHouseDataSourceFactory dsFactory = new ClickHouseDataSourceFactory();
//        dsFactory.nodeDesc = "192.168.28.201";
//        dsFactory.password = "123456";
//        dsFactory.userName = "default";
//        dsFactory.dbName = dbName;
//        dsFactory.port = 8123;
//        dsFactory.name = dbName;
//        IDataxProcessor.TableMap tableMap = new IDataxProcessor.TableMap();
//        tableMap.setFrom("application");
//        tableMap.setTo("customer_order_relation");
//
//        ISelectedTab.ColMeta cm = null;
//        List<ISelectedTab.ColMeta> cmetas = Lists.newArrayList();
//        cm = new ISelectedTab.ColMeta();
//        cm.setName("customerregister_id");
//        cm.setType(ISelectedTab.DataXReaderColType.STRING);
//        cmetas.add(cm);
//
//        cm = new ISelectedTab.ColMeta();
//        cm.setName("waitingorder_id");
//        cm.setType(ISelectedTab.DataXReaderColType.STRING);
//        cmetas.add(cm);
//
//        cm = new ISelectedTab.ColMeta();
//        cm.setName("kind");
//        cm.setType(ISelectedTab.DataXReaderColType.INT);
//        cmetas.add(cm);
//
//        cm = new ISelectedTab.ColMeta();
//        cm.setName("create_time");
//        cm.setType(ISelectedTab.DataXReaderColType.Long);
//        cmetas.add(cm);
//
//        cm = new ISelectedTab.ColMeta();
//        cm.setName("last_ver");
//        cm.setType(ISelectedTab.DataXReaderColType.INT);
//        cmetas.add(cm);
//
//        tableMap.setSourceCols(cmetas);
        ClickHouseTest forTest = createDataXWriter(); //new DataXClickhouseWriter() {
//            @Override
//            public Class<?> getOwnerClass() {
//                return DataXClickhouseWriter.class;
//            }
//
//            @Override
//            public ClickHouseDataSourceFactory getDataSourceFactory() {
//                // return super.getDataSourceFactory();
//                return dsFactory;
//            }
//        };
//        writer.template = DataXClickhouseWriter.getDftTemplate();
//        writer.batchByteSize = 3456;
//        writer.batchSize = 9527;
//        writer.dbName = dbName;
//        writer.writeMode = "insert";
//        // writer.autoCreateTable = true;
//        writer.postSql = "drop table @table";
//        writer.preSql = "drop table @table";
//
//        writer.dataXName = testDataXName;
//        writer.dbName = dbName;

        WriterTemplate.valiateCfgGenerate("clickhouse-datax-writer-assert.json", forTest.writer, forTest.tableMap);

        forTest.writer.postSql = null;
        forTest.writer.preSql = null;
        forTest.writer.batchSize = null;
        forTest.writer.batchByteSize = null;
        forTest.writer.writeMode = null;


        WriterTemplate.valiateCfgGenerate(clickhouse_datax_writer_assert_without_optional, forTest.writer, forTest.tableMap);
    }

    private static class ClickHouseTest {
        final DataXClickhouseWriter writer;
        final IDataxProcessor.TableMap tableMap;

        public ClickHouseTest(DataXClickhouseWriter writer, IDataxProcessor.TableMap tableMap) {
            this.writer = writer;
            this.tableMap = tableMap;
        }
    }

    private static ClickHouseTest createDataXWriter() {


        String dbName = "tis";
        ClickHouseDataSourceFactory dsFactory = new ClickHouseDataSourceFactory();
        dsFactory.nodeDesc = "192.168.28.201";
        dsFactory.password = "123456";
        dsFactory.userName = "default";
        dsFactory.dbName = dbName;
        dsFactory.port = 8123;
        dsFactory.name = dbName;
        IDataxProcessor.TableMap tableMap = new IDataxProcessor.TableMap();
        tableMap.setFrom("application");
        tableMap.setTo(targetTableName);

        ISelectedTab.ColMeta cm = null;
        List<ISelectedTab.ColMeta> cmetas = Lists.newArrayList();
        cm = new ISelectedTab.ColMeta();
        cm.setName("customerregister_id");
        cm.setType(ISelectedTab.DataXReaderColType.STRING.dataType);
        cmetas.add(cm);

        cm = new ISelectedTab.ColMeta();
        cm.setName("waitingorder_id");
        cm.setType(ISelectedTab.DataXReaderColType.STRING.dataType);
        cmetas.add(cm);

        cm = new ISelectedTab.ColMeta();
        cm.setName("kind");
        cm.setType(ISelectedTab.DataXReaderColType.INT.dataType);
        cmetas.add(cm);

        cm = new ISelectedTab.ColMeta();
        cm.setName("create_time");
        cm.setType(ISelectedTab.DataXReaderColType.Long.dataType);
        cmetas.add(cm);

        cm = new ISelectedTab.ColMeta();
        cm.setName("last_ver");
        cm.setType(ISelectedTab.DataXReaderColType.INT.dataType);
        cmetas.add(cm);

        tableMap.setSourceCols(cmetas);
        DataXClickhouseWriter writer = new DataXClickhouseWriter() {
            @Override
            public Class<?> getOwnerClass() {
                return DataXClickhouseWriter.class;
            }

            @Override
            public ClickHouseDataSourceFactory getDataSourceFactory() {
                // return super.getDataSourceFactory();
                return dsFactory;
            }
        };
        writer.template = DataXClickhouseWriter.getDftTemplate();
        writer.batchByteSize = 3456;
        writer.batchSize = 9527;
        writer.dbName = dbName;
        writer.writeMode = "insert";
        // writer.autoCreateTable = true;
        writer.postSql = "drop table @table";
        writer.preSql = "drop table @table";

        writer.dataXName = testDataXName;
        writer.dbName = dbName;
        return new ClickHouseTest(writer, tableMap);
    }


    public void testRealDump() throws Exception {

        ClickHouseTest houseTest = createDataXWriter();

        houseTest.writer.autoCreateTable = true;

        DataxProcessor dataXProcessor = EasyMock.mock("dataXProcessor", DataxProcessor.class);
        File createDDLDir = new File(".");
        File createDDLFile = null;
        try {
            createDDLFile = new File(createDDLDir, targetTableName + IDataxProcessor.DATAX_CREATE_DDL_FILE_NAME_SUFFIX);
            FileUtils.write(createDDLFile
                    , com.qlangtech.tis.extension.impl.IOUtils.loadResourceFromClasspath(DataXClickhouseWriter.class, "create_ddl_customer_order_relation.sql"), TisUTF8.get());

            EasyMock.expect(dataXProcessor.getDataxCreateDDLDir(null)).andReturn(createDDLDir);
            DataxWriter.dataxWriterGetter = (dataXName) -> {
                return houseTest.writer;
            };
            DataxProcessor.processorGetter = (dataXName) -> {
                assertEquals(testDataXName, dataXName);
                return dataXProcessor;
            };
            EasyMock.replay(dataXProcessor);
            DataXClickhouseWriter writer = new DataXClickhouseWriter();
            WriterTemplate.realExecuteDump(clickhouse_datax_writer_assert_without_optional, writer);

            EasyMock.verify(dataXProcessor);
        } finally {
            FileUtils.forceDelete(createDDLFile);
        }
    }

    public void testGenerateCreateDDL() {

        ClickHouseTest dataXWriter = createDataXWriter();

        StringBuffer createDDL = dataXWriter.writer.generateCreateDDL(dataXWriter.tableMap);
        assertNull(createDDL);

        dataXWriter.writer.autoCreateTable = true;
        createDDL = dataXWriter.writer.generateCreateDDL(dataXWriter.tableMap);
        assertNotNull(createDDL);

        assertEquals("CREATE TABLE customer_order_relation\n" +
                "(\n" +
                "    `customerregister_id`   String,\n" +
                "    `waitingorder_id`       String,\n" +
                "    `kind`                  Int32,\n" +
                "    `create_time`           Int64,\n" +
                "    `last_ver`              Int32\n" +
                "   ,`__cc_ck_sign` Int8 DEFAULT 1\n" +
                ")\n" +
                " ENGINE = CollapsingMergeTree(__cc_ck_sign)\n" +
                " ORDER BY \n" +
                " SETTINGS index_granularity = 8192", String.valueOf(createDDL));

        System.out.println(createDDL);
    }
}
