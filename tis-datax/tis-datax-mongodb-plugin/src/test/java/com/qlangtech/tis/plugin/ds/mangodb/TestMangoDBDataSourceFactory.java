package com.qlangtech.tis.plugin.ds.mangodb;

import com.alibaba.citrus.turbine.Context;
import com.mongodb.AuthenticationMechanism;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.plugin.ds.TableInDB;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.test.TISEasyMock;
import junit.framework.TestCase;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.Document;
import org.junit.Assert;

import java.util.List;
import java.util.Map;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/8/30
 */
public class TestMangoDBDataSourceFactory extends TestCase implements TISEasyMock {
    public void testMangoConnect() throws Exception {
        MangoDBDataSourceFactory mongoDS = createMangoDBDS();

        TableInDB tablesInDB = mongoDS.getTablesInDB();

        List<String> tabs = tablesInDB.getTabs();
        Assert.assertTrue(tabs.size() > 0);


        Descriptor descriptor = mongoDS.getDescriptor();
        Assert.assertNotNull("desc can not be null", descriptor);

        IControlMsgHandler msgHandler = mock("msgHandler", IControlMsgHandler.class);
        Context context = mock("context", Context.class);

        MangoDBDataSourceFactory.DefaultDescriptor desc = new MangoDBDataSourceFactory.DefaultDescriptor();

        replay();
        desc.validateDSFactory(msgHandler, context, mongoDS);


        MongoClient mongoClient = mongoDS.unwrap(MongoClient.class);

        MongoDatabase database = mongoClient.getDatabase(mongoDS.dbName);
        //  MongoCollection<Document> user = database.getCollection("instancedetail");
        MongoCollection<Document> user = database.getCollection("user");
        int i = 0;
        Object v = null;
        BsonValue val = null;
        BsonDocument bdoc = null;
        for (Document doc : user.find()) {
            bdoc = doc.toBsonDocument(BsonDocument.class, MongoClient.getDefaultCodecRegistry());

            for (Map.Entry<String, Object> entry : doc.entrySet()) {
                v = entry.getValue();
                System.out.println(entry.getKey() + "->" + ((v != null) ? (v + ",t:" + v.getClass().getName()) :
                        " " + "null"));
            }
            System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
            for (Map.Entry<String, BsonValue> entry : bdoc.entrySet()) {
                val = entry.getValue();
                System.out.println(entry.getKey() + "->" + ((val != null) ? (val + ",t:" + val.getBsonType()) :
                        " " + "null"));
            }
            System.out.println("----------------");

            if (i++ > 2) {
                break;
            }
        }

        verifyAll();
    }

    public static MangoDBDataSourceFactory createMangoDBDS() {
        MangoDBDataSourceFactory mongoDS = new MangoDBDataSourceFactory();
        mongoDS.username = "admin";
        mongoDS.password = "123456";
        mongoDS.address = "192.168.28.201:27017";
        mongoDS.dbName = "tis";
        mongoDS.userSource = "tis";
        mongoDS.authMechanism = AuthenticationMechanism.SCRAM_SHA_1.getMechanismName();
        return mongoDS;
    }
}
