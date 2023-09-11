package com.qlangtech.tis.plugin.datax;

import com.mongodb.AuthenticationMechanism;
import com.qlangtech.tis.plugin.common.ReaderTemplate;
import com.qlangtech.tis.plugin.ds.mangodb.MangoDBDataSourceFactory;
import junit.framework.TestCase;

import java.io.File;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/9/11
 */
public class TestDataXMongodbReaderRealDump extends TestCase {


    public void testRealDump() throws Exception {

        String dataXName = "mongoReader";
        String readerJson = "";
        File writeFile = new File("test_mongo_reader.txt");

        DataXMongodbReader mongodbReader = new DataXMongodbReader();

        ReaderTemplate.realExecute(dataXName, readerJson, writeFile, mongodbReader);
    }


    private DataXMongodbReader createMongoReader() {
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
      //  reader.selectedTabs
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
