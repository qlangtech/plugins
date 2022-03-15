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

package com.qlangtech.tis.plugins.incr.flink.connector.hudi;

import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.IDataxReader;
import com.qlangtech.tis.datax.impl.DataxProcessor;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.fs.IPath;
import com.qlangtech.tis.fs.ITISFileSystem;
import com.qlangtech.tis.hdfs.impl.HdfsPath;
import com.qlangtech.tis.hdfs.test.HdfsFileSystemFactoryTestUtils;
import com.qlangtech.tis.manage.common.CenterResource;
import com.qlangtech.tis.manage.common.TisUTF8;
import com.qlangtech.tis.offline.FileSystemFactory;
import com.qlangtech.tis.plugin.KeyedPluginStore;
import com.qlangtech.tis.plugin.datax.hudi.HudiSelectedTab;
import org.apache.commons.io.FileUtils;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Collections;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-03-14 09:05
 **/
public class TestHudiSinkFactory {
    public static final String targetTableName = "customer_order_relation";
    public static final String hudi_datax_writer_assert_without_optional = "hudi-datax-writer-assert-without-optional.json";
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @BeforeClass
    public static void startClazz() {
        CenterResource.setNotFetchFromCenterRepository();
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
        // HudiTest forTest = createDataXWriter(Optional.of(fsFactory));
        DataxProcessor dataXProcessor = EasyMock.mock("dataXProcessor", DataxProcessor.class);
        String dataXCfgFile = targetTableName + "_0" + IDataxProcessor.DATAX_CREATE_DATAX_CFG_FILE_NAME_SUFFIX;
        File dataXCfg = folder.newFile(dataXCfgFile);
        FileUtils.writeStringToFile(dataXCfg
                , "{job:{content:[{\"writer\":"
                        + IOUtils.loadResourceFromClasspath(
                        this.getClass(), hudi_datax_writer_assert_without_optional)
                        + "}]}}"
                , TisUTF8.get());

        //   List<File> dataXFiles = Lists.newArrayList(dataXCfg);

        IDataxReader dataXReader = EasyMock.createMock("dataXReader", IDataxReader.class);

        HudiSelectedTab hudiTab = new HudiSelectedTab();
        hudiTab.name = targetTableName;

        EasyMock.expect(dataXReader.getSelectedTabs()).andReturn(Collections.singletonList(hudiTab));

        EasyMock.expect(dataXProcessor.getReader(null)).andReturn(dataXReader);

        EasyMock.expect(dataXProcessor.getDataxCfgFileNames(null)).andReturn(Collections.singletonList(dataXCfg));

        DataxProcessor.processorGetter = (dataXName) -> {
            Assert.assertEquals(HdfsFileSystemFactoryTestUtils.testDataXName.getName(), dataXName);
            return dataXProcessor;
        };
        EasyMock.replay(dataXProcessor, fsFactory, fs, dataXReader);
//        IStreamTableCreator.IStreamTableMeta
//                streamTableMeta = forTest.writer.getStreamTableMeta(HudiWriter.targetTableName);

//        Assert.assertNotNull("streamTableMeta can not be null", streamTableMeta);
//        streamTableMeta.getColsMeta();

        // System.out.println(streamTableMeta.createFlinkTableDDL());

        HudiSinkFactory sinkFactory = new HudiSinkFactory();
        //String groupName, String keyVal, Class<T> pluginClass
        sinkFactory.setKey(new KeyedPluginStore.Key(null, HdfsFileSystemFactoryTestUtils.testDataXName.getName(), null));

        HudiSinkFactory.HudiStreamTemplateData tplData
                = (HudiSinkFactory.HudiStreamTemplateData) sinkFactory.decorateMergeData(
                new TestStreamTemplateData(HdfsFileSystemFactoryTestUtils.testDataXName, targetTableName));
//
//
        StringBuffer createTabDdl = tplData.getSinkFlinkTableDDL(targetTableName);


        Assert.assertNotNull(createTabDdl);
//
//        System.out.println(createTabDdl);


        EasyMock.verify(dataXProcessor, fsFactory, fs, dataXReader);
    }
}
