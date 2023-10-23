package com.qlangtech.tis.hive;

import com.qlangtech.tis.config.authtoken.impl.OffUserToken;
import com.qlangtech.tis.config.hive.meta.HiveTable;
import com.qlangtech.tis.plugin.ds.ColumnMetaData;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/10/20
 */
public class TestHiveserverAliyunDataSourceFactory {

    public void createTable(IMetaStoreClient store) throws Exception {
        String tableName = "mytable";
        int startTime = (int) (System.currentTimeMillis() / 1000);
        List<FieldSchema> cols = new ArrayList<FieldSchema>();
        cols.add(new FieldSchema("col1", "int", ""));
        SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
        Map<String, String> params = new HashMap<String, String>();
        params.put("key", "value");
        StorageDescriptor sd = new StorageDescriptor(cols, "file:/tmp", "input", "output", false, 17
                , serde, Arrays.asList("bucketcol"), Arrays.asList(new Order("sortcol", 1)), params);
        Table table = new Table(tableName, "default", "me", startTime, startTime, 0, sd, null, Collections.emptyMap(), null, null, null);
        store.createTable(table);
        Table t = store.getTable("default", tableName);
        Assert.assertEquals(1, t.getSd().getColsSize());
        Assert.assertEquals("col1", t.getSd().getCols().get(0).getName());
        Assert.assertEquals("int", t.getSd().getCols().get(0).getType());
        Assert.assertEquals("", t.getSd().getCols().get(0).getComment());
        Assert.assertEquals("serde", t.getSd().getSerdeInfo().getName());
        Assert.assertEquals("seriallib", t.getSd().getSerdeInfo().getSerializationLib());
        Assert.assertEquals("file:/tmp", t.getSd().getLocation());
        Assert.assertEquals("input", t.getSd().getInputFormat());
        Assert.assertEquals("output", t.getSd().getOutputFormat());
        Assert.assertFalse(t.getSd().isCompressed());
        Assert.assertEquals(17, t.getSd().getNumBuckets());
        Assert.assertEquals(1, t.getSd().getBucketColsSize());
        Assert.assertEquals("bucketcol", t.getSd().getBucketCols().get(0));
        Assert.assertEquals(1, t.getSd().getSortColsSize());
        Assert.assertEquals("sortcol", t.getSd().getSortCols().get(0).getCol());
        Assert.assertEquals(1, t.getSd().getSortCols().get(0).getOrder());
        Assert.assertEquals(1, t.getSd().getParametersSize());
        Assert.assertEquals("value", t.getSd().getParameters().get("key"));
        Assert.assertEquals("me", t.getOwner());
        Assert.assertEquals("default", t.getDbName());
        Assert.assertEquals(tableName, t.getTableName());
        Assert.assertEquals(3, t.getParametersSize());
    }

    @Test
    public void testCreateMeta() throws Exception {
        //  Hiveserver2DataSourceFactory hsource = new Hiveserver2DataSourceFactory();

        Hiveserver2DataSourceFactory hiveDS = new Hiveserver2DataSourceFactory();
        hiveDS.dbName = "default";
        HiveMeta meta = new HiveMeta();
        meta.metaStoreUrls = "thrift://47.98.42.34:9083";
        meta.userToken = new OffUserToken();
        //  hiveDS.metadata = createKerberToken();
        hiveDS.metadata = meta;
        //  hiveDS.userToken = createKerberToken();

        try (DefaultHiveMetaStore metaStore = (DefaultHiveMetaStore) hiveDS.createMetaStoreClient()) {

            IMetaStoreClient storeClient = metaStore.storeClient;
            Database db = storeClient.getDatabase("default");
            System.out.println(db.getLocationUri());
            //   createTable(storeClient);

//            Table tbl = new Table();
//            tbl.setTableName("test_tab");
//            tbl.setParameters(Maps.newHashMap());
//            tbl.setDbName(hiveDS.dbName);
//            StorageDescriptor sd = new StorageDescriptor();
//            SerDeInfo seDeInfo = new SerDeInfo();
//            seDeInfo.set
//            sd.setSerdeInfo(seDeInfo);
//            sd.setParameters(Maps.newHashMap());
//            sd.setLocation("oss://tis-hdfs/user/admin/default/test_tab");
//            FieldSchema field = new FieldSchema();
//            field.setName("user_id");
//            field.setType("bigint");
//            sd.addToCols(field);
//            tbl.setSd(sd);
//            storeClient.createTable(tbl);

            Assert.assertNotNull("metaStore can not be null", metaStore);
            List<HiveTable> tabs = metaStore.getTables("default");

            Assert.assertNotNull("tabs can not be null", tabs);
            System.out.println("tabs size:" + tabs.size());
            for (HiveTable tab : tabs) {
                System.out.println(tab.getTableName());
            }
        }


        Hms hms = new Hms();
        hms.hiveAddress = "47.98.42.34:10000";
        hms.userToken = new OffUserToken();
        hiveDS.hms = hms;

        List<ColumnMetaData> metas = hiveDS.getTableMetadata(false, EntityName.parse("payinfo"));
        for (ColumnMetaData m : metas) {
            System.out.println(m.toString());
        }

//        hiveDS.visitFirstConnection((conn) -> {
//
//
//            try {
//
//                conn.execute("CREATE EXTERNAL TABLE IF NOT EXISTS default.`orderdefail`\n" +
//                        "(\n" +
//                        "    `pay_id`             VARCHAR(32)\n" +
//                        ")\n" +
//                        "COMMENT 'tis_tmp_payinfo' PARTITIONED BY(pt string,pmod string)   \n" +
//                        "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' with SERDEPROPERTIES ('serialization.null.format'='\\\\N', 'line.delim' ='\n" +
//                        "','field.delim'='\u0001')\n" +
//                        "STORED AS TEXTFILE\n" +
//                        "LOCATION 'oss://tis-hdfs/user/admin/default/orderdefail'");
//
//                conn.query("show tables", (result) -> {
//                    System.out.println("table:" + result.getString(1));
//                    return true;
//                });
//            } catch (Exception e) {
//                throw new RuntimeException(e);
//            }
//        });

    }
}
