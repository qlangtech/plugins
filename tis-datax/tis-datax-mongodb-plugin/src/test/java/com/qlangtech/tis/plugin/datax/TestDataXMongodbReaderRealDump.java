package com.qlangtech.tis.plugin.datax;

import com.mongodb.AuthenticationMechanism;
import com.qlangtech.tis.datax.impl.DataxProcessor;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.extension.impl.XmlFile;
import com.qlangtech.tis.manage.common.TisUTF8;
import com.qlangtech.tis.plugin.PluginStore;
import com.qlangtech.tis.plugin.common.ReaderTemplate;
import com.qlangtech.tis.plugin.ds.mangodb.MangoDBDataSourceFactory;
import org.apache.commons.io.FileUtils;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Collections;
import java.util.Objects;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/9/11
 */
public class TestDataXMongodbReaderRealDump {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testRealDump() throws Exception {


        String dataXName = "mongoReader";
        String readerJson = "mongo-datax-reader-cfg.json";
        File writeFile = folder.newFile("test_mongo_assert_reader.txt"); // new File("");

        DataXMongodbReader mongodbReader = createMongoReader();
        DataxProcessor dataXProcessor = EasyMock.mock("dataXProcessor", DataxProcessor.class);
        EasyMock.expect(dataXProcessor.getReader(null)).andReturn(mongodbReader).times(3);
        DataxProcessor.processorGetter = (xname) -> {
            Assert.assertEquals(dataXName, xname);
            return dataXProcessor;
        };
        EasyMock.replay(dataXProcessor);
        ReaderTemplate.realExecute(dataXName, readerJson, writeFile, mongodbReader);
        EasyMock.verify(dataXProcessor);


        System.out.println(FileUtils.readFileToString(writeFile, TisUTF8.get()));
    }


    private DataXMongodbReader createMongoReader() throws Exception {
        MangoDBDataSourceFactory dsFactory = createMangoDBDS();
        DataXMongodbReader reader = new DataXMongodbReader() {
            @Override
            public MangoDBDataSourceFactory getDataSourceFactory() {
                return dsFactory;
            }

            @Override
            public Class<?> getOwnerClass() {
                return DataXMongodbReader.class;
            }
        };

        File selTabs = folder.newFile("selected-tabs.xml");
        IOUtils.loadResourceFromClasspath(TestDataXMongodbReaderRealDump.class
                , "mongo-datax-reader-cfg-selected-tabs.xml", true, (input) -> {
                    FileUtils.copyInputStreamToFile(input, selTabs);
                    return null;
                });


        PluginStore<SelectedTab> tabsStore = new PluginStore<>(SelectedTab.class, new XmlFile(selTabs));
        reader.selectedTabs = Collections.singletonList(Objects.requireNonNull(tabsStore.getPlugin(), "select tab can not be null"));
        reader.selectedTabs.forEach((tab) -> {
            TestDataXMongodbReader.setMongoSourceTabExtend(tab);
        });
        return reader;
    }

    private static MangoDBDataSourceFactory createMangoDBDS() {
        MangoDBDataSourceFactory mongoDS = new MangoDBDataSourceFactory();
        mongoDS.name = "testMongoDS";
        mongoDS.username = "admin";
        mongoDS.password = "123456";
        mongoDS.address = "192.168.28.201:27017";
        mongoDS.dbName = "tis";
        mongoDS.userSource = "tis";
        mongoDS.authMechanism = AuthenticationMechanism.SCRAM_SHA_1.getMechanismName();
        return mongoDS;
    }

}
