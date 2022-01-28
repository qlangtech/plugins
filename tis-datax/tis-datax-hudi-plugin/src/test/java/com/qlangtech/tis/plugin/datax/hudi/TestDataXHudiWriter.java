/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.qlangtech.tis.plugin.datax.hudi;

import com.alibaba.datax.plugin.writer.hudi.HudiWriter;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.config.hive.IHiveConnGetter;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.impl.DataxProcessor;
import com.qlangtech.tis.datax.impl.DataxWriter;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.hdfs.test.HdfsFileSystemFactoryTestUtils;
import com.qlangtech.tis.manage.common.TisUTF8;
import com.qlangtech.tis.offline.FileSystemFactory;
import com.qlangtech.tis.plugin.common.WriterTemplate;
import org.apache.commons.io.FileUtils;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;


/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-01-24 10:20
 **/
public class TestDataXHudiWriter {

    // private static final String targetTableName ="";
    private static final String clickhouse_datax_writer_assert_without_optional = "hudi-datax-writer-assert-without-optional.json";


    @Test
    public void testRealDump() throws Exception {




        HudiTest houseTest = createDataXWriter();

        houseTest.writer.autoCreateTable = true;

        DataxProcessor dataXProcessor = EasyMock.mock("dataXProcessor", DataxProcessor.class);
        File createDDLDir = new File(".");
        File createDDLFile = null;
        try {
            createDDLFile = new File(createDDLDir, HudiWriter.targetTableName + IDataxProcessor.DATAX_CREATE_DDL_FILE_NAME_SUFFIX);
            FileUtils.write(createDDLFile
                    , com.qlangtech.tis.extension.impl.IOUtils.loadResourceFromClasspath(DataXHudiWriter.class
                            , "create_ddl_customer_order_relation.sql"), TisUTF8.get());

            // EasyMock.expect(dataXProcessor.getDataxCreateDDLDir(null)).andReturn(createDDLDir);
            DataxWriter.dataxWriterGetter = (dataXName) -> {
                return houseTest.writer;
            };
            DataxProcessor.processorGetter = (dataXName) -> {
                Assert.assertEquals(HdfsFileSystemFactoryTestUtils.testDataXName, dataXName);
                return dataXProcessor;
            };
            EasyMock.replay(dataXProcessor);
            // DataXHudiWriter writer = new DataXHudiWriter();
            WriterTemplate.realExecuteDump(clickhouse_datax_writer_assert_without_optional, houseTest.writer);

            EasyMock.verify(dataXProcessor);
        } finally {
            FileUtils.deleteQuietly(createDDLFile);
        }
    }

    private static class HudiTest {
        private final DataXHudiWriter writer;
        private final IDataxProcessor.TableMap tableMap;

        public HudiTest(DataXHudiWriter writer, IDataxProcessor.TableMap tableMap) {
            this.writer = writer;
            this.tableMap = tableMap;
        }
    }


    private static HudiTest createDataXWriter() {


        String dbName = "tis";
//        ClickHouseDataSourceFactory dsFactory = new ClickHouseDataSourceFactory();
//        dsFactory.nodeDesc = "192.168.28.201";
//        dsFactory.password = "123456";
//        dsFactory.userName = "default";
//        dsFactory.dbName = dbName;
//        dsFactory.port = 8123;
//        dsFactory.name = dbName;
        // IDataxProcessor.TableMap tableMap = ;// Template new IDataxProcessor.TableMap();
//        tableMap.setFrom("application");
//        tableMap.setTo(targetTableName);
//
//        ISelectedTab.ColMeta cm = null;
//        List<ISelectedTab.ColMeta> cmetas = Lists.newArrayList();
//        cm = new ISelectedTab.ColMeta();
//        cm.setPk(true);
//        cm.setName("customerregister_id");
//        cm.setType(ISelectedTab.DataXReaderColType.STRING.dataType);
//        cmetas.add(cm);
//
//        cm = new ISelectedTab.ColMeta();
//        cm.setName("waitingorder_id");
//        cm.setType(ISelectedTab.DataXReaderColType.STRING.dataType);
//        cmetas.add(cm);
//
//        cm = new ISelectedTab.ColMeta();
//        cm.setName("kind");
//        cm.setType(ISelectedTab.DataXReaderColType.INT.dataType);
//        cmetas.add(cm);
//
//        cm = new ISelectedTab.ColMeta();
//        cm.setName("create_time");
//        cm.setType(ISelectedTab.DataXReaderColType.Long.dataType);
//        cmetas.add(cm);
//
//        cm = new ISelectedTab.ColMeta();
//        cm.setName("last_ver");
//        cm.setType(ISelectedTab.DataXReaderColType.INT.dataType);
//        cmetas.add(cm);
//
//        tableMap.setSourceCols(cmetas);
        DataXHudiWriter writer = new DataXHudiWriter() {
            @Override
            public Class<?> getOwnerClass() {
                return DataXHudiWriter.class;
            }

            public IHiveConnGetter getHiveConnGetter() {
                return createHiveConnGetter();
            }

            @Override
            public FileSystemFactory getFs() {

                return HdfsFileSystemFactoryTestUtils.getFileSystemFactory();
            }
        };
        writer.template = DataXHudiWriter.getDftTemplate();
        writer.fsName = HdfsFileSystemFactoryTestUtils.FS_NAME;
//        writer.batchByteSize = 3456;
//        writer.batchSize = 9527;
//        writer.dbName = dbName;
        writer.writeMode = "insert";
        // writer.autoCreateTable = true;
//        writer.postSql = "drop table @table";
//        writer.preSql = "drop table @table";

       // writer.dataXName = HdfsFileSystemFactoryTestUtils.testDataXName.getName();
        //  writer.dbName = dbName;
        return new HudiTest(writer, WriterTemplate.createCustomer_order_relationTableMap());
    }

//    @NotNull
//    private static FileSystemFactory createFileSystemFactory() {
//        HdfsFileSystemFactory hdfsFactory = new HdfsFileSystemFactory();
//        hdfsFactory.name = FS_NAME;
//        hdfsFactory.rootDir = "/user/admin";
//        hdfsFactory.hdfsAddress = "hdfs://daily-cdh201";
//        hdfsFactory.hdfsSiteContent
//                = IOUtils.loadResourceFromClasspath(TestDataXHudiWriter.class, "hdfsSiteContent.xml");
//        hdfsFactory.userHostname = true;
//        return hdfsFactory;
//    }

    private static IHiveConnGetter createHiveConnGetter() {
        Descriptor hiveConnGetter = TIS.get().getDescriptor("DefaultHiveConnGetter");
        Assert.assertNotNull(hiveConnGetter);

        // 使用hudi的docker运行环境 https://hudi.apache.org/docs/docker_demo#step-3-sync-with-hive
        Descriptor.FormData formData = new Descriptor.FormData();
        formData.addProp("name", "testhiveConn");
        formData.addProp("hiveAddress", "hiveserver:10000");

        formData.addProp("useUserToken", "true");
        formData.addProp("dbName", "default");
        formData.addProp("password", "hive");
        formData.addProp("userName", "hive");


        Descriptor.ParseDescribable<IHiveConnGetter> parseDescribable
                = hiveConnGetter.newInstance(HdfsFileSystemFactoryTestUtils.testDataXName.getName(), formData);
        Assert.assertNotNull(parseDescribable.instance);

        Assert.assertNotNull(parseDescribable.instance);
        return parseDescribable.instance;
    }
}
