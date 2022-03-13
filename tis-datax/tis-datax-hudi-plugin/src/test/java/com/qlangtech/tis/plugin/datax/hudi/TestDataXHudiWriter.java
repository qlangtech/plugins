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

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.hdfswriter.HdfsColMeta;
import com.alibaba.datax.plugin.writer.hudi.HudiWriter;
import com.google.common.collect.Lists;
import com.qlangtech.tis.config.hive.IHiveConnGetter;
import com.qlangtech.tis.config.hive.meta.HiveTable;
import com.qlangtech.tis.config.hive.meta.IHiveMetaStore;
import com.qlangtech.tis.config.spark.ISparkConnGetter;
import com.qlangtech.tis.config.spark.impl.DefaultSparkConnGetter;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.impl.DataXCfgGenerator;
import com.qlangtech.tis.datax.impl.DataxProcessor;
import com.qlangtech.tis.datax.impl.DataxWriter;
import com.qlangtech.tis.exec.IExecChainContext;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.fs.IPath;
import com.qlangtech.tis.fs.ITISFileSystem;
import com.qlangtech.tis.hdfs.impl.HdfsPath;
import com.qlangtech.tis.hdfs.test.HdfsFileSystemFactoryTestUtils;
import com.qlangtech.tis.manage.common.CenterResource;
import com.qlangtech.tis.manage.common.TISCollectionUtils;
import com.qlangtech.tis.manage.common.TisUTF8;
import com.qlangtech.tis.offline.DataxUtils;
import com.qlangtech.tis.offline.FileSystemFactory;
import com.qlangtech.tis.order.center.IParamContext;
import com.qlangtech.tis.plugin.KeyedPluginStore;
import com.qlangtech.tis.plugin.common.WriterTemplate;
import com.qlangtech.tis.plugin.datax.hudi.partition.OffPartition;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;
import com.qlangtech.tis.sql.parser.tuple.creator.IStreamIncrGenerateStrategy;
import org.apache.commons.io.FileUtils;
import org.easymock.EasyMock;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.slf4j.MDC;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;


/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-01-24 10:20
 **/
@Ignore
public class TestDataXHudiWriter {

    // private static final String targetTableName ="";
    public static final String hudi_datax_writer_assert_without_optional = "hudi-datax-writer-assert-without-optional.json";
    static final String cfgPathParameter = "parameter";
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();


    @BeforeClass
    public static void start() {
        CenterResource.setNotFetchFromCenterRepository();

    }

    @Test
    public void testRealDump() throws Exception {

        MDC.put(TISCollectionUtils.KEY_COLLECTION
                , HdfsFileSystemFactoryTestUtils.testDataXName.getName());
        MDC.put(IParamContext.KEY_TASK_ID, "123");
        HudiTest houseTest = createDataXWriter();
        long timestamp = 20220311135455l;

        // houseTest.writer.autoCreateTable = true;

        DataxProcessor dataXProcessor = EasyMock.mock("dataXProcessor", DataxProcessor.class);

        File dataXCfgDir = folder.newFolder();
        File createDDLDir = folder.newFolder();
        File createDDLFile = null;
        try {
            createDDLFile = new File(createDDLDir, HudiWriter.targetTableName + IDataxProcessor.DATAX_CREATE_DDL_FILE_NAME_SUFFIX);
            FileUtils.write(createDDLFile
                    , com.qlangtech.tis.extension.impl.IOUtils.loadResourceFromClasspath(DataXHudiWriter.class
                            , "create_ddl_customer_order_relation.sql"), TisUTF8.get());

            DataXCfgGenerator.GenerateCfgs genCfg = new DataXCfgGenerator.GenerateCfgs();
            genCfg.setGenTime(timestamp);
            genCfg.setGroupedChildTask(Collections.singletonMap(WriterTemplate.TAB_customer_order_relation
                    , Lists.newArrayList(WriterTemplate.TAB_customer_order_relation + "_0")));
            genCfg.write2GenFile(dataXCfgDir);

            EasyMock.expect(dataXProcessor.getDataxCfgDir(null)).andReturn(dataXCfgDir);
            // EasyMock.expect(dataXProcessor.getDataxCreateDDLDir(null)).andReturn(createDDLDir);
            DataxWriter.dataxWriterGetter = (dataXName) -> {
                return houseTest.writer;
            };
            DataxProcessor.processorGetter = (dataXName) -> {
                Assert.assertEquals(HdfsFileSystemFactoryTestUtils.testDataXName.getName(), dataXName);
                return dataXProcessor;
            };


            IExecChainContext execContext = EasyMock.mock("execContext", IExecChainContext.class);
            EasyMock.expect(execContext.getPartitionTimestamp()).andReturn(String.valueOf(timestamp));


            EasyMock.replay(dataXProcessor, execContext);

            WriterTemplate.realExecuteDump(hudi_datax_writer_assert_without_optional, houseTest.writer, (cfg) -> {
                cfg.set(cfgPathParameter + "." + DataxUtils.EXEC_TIMESTAMP, timestamp);
                return cfg;
            });


            // DataXHudiWriter hudiWriter = new DataXHudiWriter();
//            hudiWriter.dataXName = HdfsFileSystemFactoryTestUtils.testDataXName.getName();
//            hudiWriter.createPostTask(execContext, tab);

            HudiDumpPostTask postTask = (HudiDumpPostTask) houseTest.writer.createPostTask(execContext, houseTest.tab);
            Assert.assertNotNull("postTask can not be null", postTask);
            postTask.run();

            IHiveConnGetter hiveConnMeta = houseTest.writer.getHiveConnMeta();
            try (IHiveMetaStore metaStoreClient = hiveConnMeta.createMetaStoreClient()) {
                Assert.assertNotNull(metaStoreClient);
                HiveTable table = metaStoreClient.getTable(hiveConnMeta.getDbName(), WriterTemplate.TAB_customer_order_relation);
                Assert.assertNotNull(WriterTemplate.TAB_customer_order_relation + " can not be null", table);
            }

            EasyMock.verify(dataXProcessor, execContext);
        } finally {
            //  FileUtils.deleteQuietly(createDDLFile);
        }
    }

    @Test
    public void testConfigGenerate() throws Exception {

        HudiTest forTest = createDataXWriter();
        WriterTemplate.valiateCfgGenerate("hudi-datax-writer-assert.json", forTest.writer, forTest.tableMap);
    }

    @Test
    public void testFlinkSqlTableDDLCreate() throws Exception {
        FileSystemFactory fsFactory = EasyMock.createMock("fsFactory", FileSystemFactory.class);

        ITISFileSystem fs = EasyMock.createMock("fileSystem", ITISFileSystem.class);
        //  fs.getRootDir()
        String child = "default/customer_order_relation";
        String dataDir = "hudi";
        IPath rootPath = new HdfsPath(HdfsFileSystemFactoryTestUtils.DEFAULT_HDFS_ADDRESS + "/user/admin");
        IPath tabPath = new HdfsPath(rootPath, child);
        IPath hudiDataPath = new HdfsPath(tabPath, dataDir);
        EasyMock.expect(fs.getPath(rootPath, child)).andReturn(tabPath);
        EasyMock.expect(fs.getPath(tabPath, dataDir)).andReturn(hudiDataPath);
        EasyMock.expect(fs.getRootDir()).andReturn(rootPath);
        EasyMock.expect(fsFactory.getFileSystem()).andReturn(fs);
        HudiTest forTest = createDataXWriter(Optional.of(fsFactory));
        DataxProcessor dataXProcessor = EasyMock.mock("dataXProcessor", DataxProcessor.class);
        File dataXCfg = folder.newFile();
        FileUtils.writeStringToFile(dataXCfg
                , "{job:{content:[{\"writer\":"
                        + IOUtils.loadResourceFromClasspath(
                        this.getClass(), hudi_datax_writer_assert_without_optional)
                        + "}]}}"
                , TisUTF8.get());

        List<File> dataXFiles = Lists.newArrayList(dataXCfg);

        EasyMock.expect(dataXProcessor.getDataxCfgFileNames(null)).andReturn(dataXFiles);

        DataxProcessor.processorGetter = (dataXName) -> {
            Assert.assertEquals(HdfsFileSystemFactoryTestUtils.testDataXName.getName(), dataXName);
            return dataXProcessor;
        };
        EasyMock.replay(dataXProcessor, fsFactory, fs);
//        IStreamTableCreator.IStreamTableMeta
//                streamTableMeta = forTest.writer.getStreamTableMeta(HudiWriter.targetTableName);

//        Assert.assertNotNull("streamTableMeta can not be null", streamTableMeta);
//        streamTableMeta.getColsMeta();

        // System.out.println(streamTableMeta.createFlinkTableDDL());

//        DataXHudiWriter.HudiStreamTemplateData tplData
//                = (DataXHudiWriter.HudiStreamTemplateData) forTest.writer.decorateMergeData(
//                new TestStreamTemplateData(HdfsFileSystemFactoryTestUtils.testDataXName, HudiWriter.targetTableName));
//
//
//        StringBuffer createTabDdl = tplData.getSinkFlinkTableDDL(HudiWriter.targetTableName);


//        Assert.assertNotNull(createTabDdl);
//
//        System.out.println(createTabDdl);


        EasyMock.verify(dataXProcessor, fsFactory, fs);
    }

    private static class TestStreamTemplateData implements IStreamIncrGenerateStrategy.IStreamTemplateData {
        private final TargetResName collection;
        private final String targetTableName;

        public TestStreamTemplateData(TargetResName collection, String targetTableName) {
            this.collection = collection;
            this.targetTableName = targetTableName;
        }

        @Override
        public String getCollection() {
            return this.collection.getName();
        }

        @Override
        public List<EntityName> getDumpTables() {
            return Collections.singletonList(EntityName.parse(this.targetTableName));
        }
    }

    private static class HudiTest {
        private final DataXHudiWriter writer;
        private final IDataxProcessor.TableMap tableMap;
        private final HudiSelectedTab tab;

        public HudiTest(DataXHudiWriter writer, IDataxProcessor.TableMap tableMap, HudiSelectedTab tab) {
            this.writer = writer;
            this.tableMap = tableMap;
            this.tab = tab;
        }
    }

    private static HudiTest createDataXWriter() {
        return createDataXWriter(Optional.empty());
    }

    private static HudiTest createDataXWriter(Optional<FileSystemFactory> fsFactory) {

        final DefaultSparkConnGetter sparkConnGetter = new DefaultSparkConnGetter();
        sparkConnGetter.name = "default";
        sparkConnGetter.master = "spark://sparkmaster:7077";

        DataXHudiWriter writer = new DataXHudiWriter() {
            @Override
            public Class<?> getOwnerClass() {
                return DataXHudiWriter.class;
            }

            @Override
            public IHiveConnGetter getHiveConnMeta() {
                return HdfsFileSystemFactoryTestUtils.createHiveConnGetter();
            }

            @Override
            public ISparkConnGetter getSparkConnGetter() {
                return sparkConnGetter;
            }

            @Override
            public FileSystemFactory getFs() {
                return fsFactory.isPresent() ? fsFactory.get() : HdfsFileSystemFactoryTestUtils.getFileSystemFactory();
            }
        };
        writer.template = DataXHudiWriter.getDftTemplate();
        writer.fsName = HdfsFileSystemFactoryTestUtils.FS_NAME;
        writer.setKey(new KeyedPluginStore.Key(null, HdfsFileSystemFactoryTestUtils.testDataXName.getName(), null));
        writer.tabType = HudiWriteTabType.COW.getValue();
        writer.batchOp = BatchOpMode.BULK_INSERT.getValue();
        writer.shuffleParallelism = 3;
        writer.partitionedBy = "pt";

//        writer.batchByteSize = 3456;
//        writer.batchSize = 9527;
//        writer.dbName = dbName;
        writer.writeMode = "insert";
        // writer.autoCreateTable = true;
//        writer.postSql = "drop table @table";
//        writer.preSql = "drop table @table";

        // writer.dataXName = HdfsFileSystemFactoryTestUtils.testDataXName.getName();
        //  writer.dbName = dbName;

//        HudiSelectedTab hudiTab = new HudiSelectedTab() {
//            @Override
//            public List<ColMeta> getCols() {
//                return WriterTemplate.createColMetas();
//            }
//        };
//        //hudiTab.partitionPathField = WriterTemplate.kind;
//        hudiTab.recordField = WriterTemplate.customerregisterId;
//        hudiTab.sourceOrderingField = WriterTemplate.lastVer;
//        hudiTab.setWhere("1=1");
//        hudiTab.name = WriterTemplate.TAB_customer_order_relation;


        List<HdfsColMeta> colsMeta
                = HdfsColMeta.getColsMeta(Configuration.from(IOUtils.loadResourceFromClasspath(writer.getClass()
                , hudi_datax_writer_assert_without_optional)).getConfiguration(cfgPathParameter));
        HudiSelectedTab tab = new HudiSelectedTab() {
            @Override
            public List<ColMeta> getCols() {
                return colsMeta.stream().map((c) -> {
                    ColMeta col = new ColMeta();
                    col.setName(c.getName());
                    col.setPk(c.pk);
                    col.setType(c.type);
                    col.setNullable(c.nullable);
                    return col;
                }).collect(Collectors.toList());
            }
        };
        tab.name = WriterTemplate.TAB_customer_order_relation;
        tab.partition = new OffPartition();
        tab.sourceOrderingField = "last_ver";
        tab.recordField = "customerregister_id";


        return new HudiTest(writer, WriterTemplate.createCustomer_order_relationTableMap(Optional.of(tab)), tab);
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

//    private static IHiveConnGetter createHiveConnGetter() {
//        Descriptor hiveConnGetter = TIS.get().getDescriptor("DefaultHiveConnGetter");
//        Assert.assertNotNull(hiveConnGetter);
//
//        // 使用hudi的docker运行环境 https://hudi.apache.org/docs/docker_demo#step-3-sync-with-hive
//        Descriptor.FormData formData = new Descriptor.FormData();
//        formData.addProp("name", "testhiveConn");
//        formData.addProp("hiveAddress", "hiveserver:10000");
//
//        formData.addProp("useUserToken", "true");
//        formData.addProp("dbName", "default");
//        formData.addProp("password", "hive");
//        formData.addProp("userName", "hive");
//        formData.addProp("metaStoreUrls","thrift://hiveserver:9083");
//
//
//        Descriptor.ParseDescribable<IHiveConnGetter> parseDescribable
//                = hiveConnGetter.newInstance(HdfsFileSystemFactoryTestUtils.testDataXName.getName(), formData);
//        Assert.assertNotNull(parseDescribable.instance);
//
//        Assert.assertNotNull(parseDescribable.instance);
//        return parseDescribable.instance;
//    }
}
