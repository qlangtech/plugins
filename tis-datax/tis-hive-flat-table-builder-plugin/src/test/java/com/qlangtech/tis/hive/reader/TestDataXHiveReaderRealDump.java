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

package com.qlangtech.tis.hive.reader;

import com.alibaba.datax.common.spi.IDataXCfg;
import com.alibaba.datax.common.util.Configuration;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.config.authtoken.UserTokenUtils;
import com.qlangtech.tis.datax.IDataxGlobalCfg;
import com.qlangtech.tis.datax.StoreResourceTypeConstants;
import com.qlangtech.tis.datax.impl.DataxProcessor;
import com.qlangtech.tis.datax.impl.DataxReader;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.hdfs.impl.HdfsFileSystemFactory;
import com.qlangtech.tis.hdfs.impl.HdfsPath;
import com.qlangtech.tis.hive.Hiveserver2DataSourceFactory;
import com.qlangtech.tis.hive.reader.impl.DefaultPartitionFilter;
import com.qlangtech.tis.hive.reader.impl.NonePartition;
import com.qlangtech.tis.manage.common.TisUTF8;
import com.qlangtech.tis.offline.DataxUtils;
import com.qlangtech.tis.offline.FileSystemFactory;
import com.qlangtech.tis.plugin.common.ReaderTemplate;
import com.qlangtech.tis.plugin.datax.DataXGlobalConfig;
import com.qlangtech.tis.plugin.datax.DefaultDataxProcessor;
import com.qlangtech.tis.plugin.datax.SelectedTab;
import com.qlangtech.tis.plugin.datax.TestDataXHiveWriterDump;
import com.qlangtech.tis.plugin.ds.CMeta;
import com.qlangtech.tis.plugin.ds.DSKey;
import com.qlangtech.tis.plugin.ds.DataSourceFactory;
import com.qlangtech.tis.plugin.ds.DataSourceFactoryPluginStore;
import com.qlangtech.tis.plugin.ds.JDBCTypes;
import junit.framework.TestCase;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.junit.Assert;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-08-20 09:00
 **/
public class TestDataXHiveReaderRealDump extends TestCase {

    private static String dataXName = "test";
    // private static final String NAME_TAB = "instancedetail";

    private static final String NAME_TAB = "customer_transactions";

//    public void testGetHdfsPath() throws Exception {
//        DataXHiveReader dataxReader = createReader(dataXName);
//        HiveDFSLinker dfsLinker = dataxReader.getDfsLinker();
//        FileSystemFactory fs = dfsLinker.getFs();
//        try (InputStream out = fs.getFileSystem()
//                .open(new HdfsPath("hdfs://192.168.28.200:30070/user/hive/warehouse/instancedetail/000000_0"))) {
//            System.out.println(org.apache.commons.io.IOUtils.toString(out));
//        }
//    }


    // @Test
    public void testRealDump() throws Exception {
        DataxProcessor.processorGetter = (dataXName) -> {
            DefaultDataxProcessor processor = new DefaultDataxProcessor() {
                @Override
                public IDataxGlobalCfg getDataXGlobalCfg() {
                    return new DataXGlobalConfig();
                }
            };
            processor.name = dataXName;
            return processor;
        };
        Hiveserver2DataSourceFactory dsFactory = TestDataXHiveWriterDump.createHiveserver2DataSourceFactory(UserTokenUtils.createNoneAuthToken());
        dsFactory.visitFirstConnection((conn) -> {
//            try {
//                conn.execute("drop table " + NAME_TAB);
//                conn.execute("CREATE TABLE `" + NAME_TAB + "`(                              \n" +
//                        "   `foo` int,                                       \n" +
//                        "   `bar` string)                                    \n" +
//                        " ROW FORMAT SERDE                                   \n" +
//                        "   'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'  \n" +
//                        " STORED AS INPUTFORMAT                              \n" +
//                        "   'org.apache.hadoop.mapred.TextInputFormat'       \n" +
//                        " OUTPUTFORMAT                                       \n" +
//                        "   'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' \n" +
//                        " LOCATION                                           \n" +
//                        "   'hdfs://192.168.28.200:30070/user/hive/warehouse/" + NAME_TAB + "' \n" +
//                        " TBLPROPERTIES (                                    \n" +
//                        "   'transient_lastDdlTime'='1730453007')");
//                conn.execute("insert into " + NAME_TAB + "(foo,bar) values (1,'name1'),(2,'name2'),(3,'name3'),(4,'name4')");
//            } catch (Exception e) {
//                throw new RuntimeException(e);
//            }

//            try {
//                conn.execute("drop table " + "customer_transactions");
//                conn.execute("CREATE TABLE customer_transactions (\n" +
//                        "    transaction_id INT,\n" +
//                        "    customer_id INT,\n" +
//                        "    amount DECIMAL(10,2),\n" +
//                        "    product_code STRING,\n" +
//                        "    transaction_date TIMESTAMP\n" +
//                        ")\n" +
//                        "STORED AS PARQUET\n" +
//                        "LOCATION                                           \n" +
//                        "   'hdfs://192.168.28.200:30070/user/hive/warehouse/customer_transactions' \n" +
//                        "TBLPROPERTIES (\n" +
//                        "    'parquet.compression'='SNAPPY',          -- 设置压缩算法为Snappy\n" +
//                        "    'parquet.block.size'='134217728',        -- 设置块大小为128MB\n" +
//                        "    'parquet.page.size'='1048576',           -- 设置页面大小为1MB\n" +
//                        "    'parquet.dictionary.enabled'='TRUE',     -- 启用字典编码\n" +
//                        "    'parquet.enable.dictionary'='TRUE',      -- 启用字典编码（重复参数，但确保生效）\n" +
//                        "    'parquet.write.support'='org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'  -- 指定输出格式类\n" +
//                        ")");
//
//                conn.execute("INSERT INTO customer_transactions (transaction_id, customer_id, amount, product_code, transaction_date)\n" +
//                        "VALUES \n" +
//                        "(1, 101, 150.00, 'A123', '2024-01-01 00:00:00'),\n" +
//                        "(2, 102, 200.50, 'B456', '2024-01-02 00:00:00'),\n" +
//                        "(3, 103, 75.25, 'C789', '2024-01-03 00:00:00'),\n" +
//                        "(4, 104, 300.00, 'D101', '2024-01-04 00:00:00'),\n" +
//                        "(5, 105, 50.00, 'E102', '2024-01-05 00:00:00')");
//            } catch (Exception e) {
//                throw new RuntimeException(e);
//            }

        });
        TIS.dsFactoryPluginStoreGetter = (p) -> {
            DSKey key = new DSKey(StoreResourceTypeConstants.DB_GROUP_NAME, p, DataSourceFactory.class);
            return new DataSourceFactoryPluginStore(key, false) {
                @Override
                public DataSourceFactory getPlugin() {
                    return dsFactory;
                }
            };
        };
        File dataxReaderResult = new File("./mysql-datax-reader-result.txt");
        FileUtils.touch(dataxReaderResult);
        try {
            DataXHiveReader dataxReader = createReader(dataXName);
            DataxReader.dataxReaderGetter = (name) -> {
                Assert.assertEquals(TestDataXHiveReaderRealDump.dataXName, name);
                return dataxReader;
            };

            Configuration readerConf = IOUtils.loadResourceFromClasspath(dataxReader.getClass() //
                    , "hive-datax-reader-test-cfg.json", true, (writerJsonInput) -> {
                        return Configuration.from(writerJsonInput);
                    });
            readerConf.set("parameter.entityName", NAME_TAB);
            readerConf.set("parameter.connection[0].jdbcUrl[0]", dsFactory.getJdbcUrls().get(0));
            readerConf.set(IDataXCfg.connectKeyParameter + "." + DataxUtils.DATASOURCE_FACTORY_IDENTITY,
                    dsFactory.identityValue());
            ReaderTemplate.realExecute(dataXName, readerConf, dataxReaderResult, dataxReader);
            System.out.println(FileUtils.readFileToString(dataxReaderResult, TisUTF8.get()));
        } finally {
            org.apache.commons.io.FileUtils.deleteQuietly(dataxReaderResult);
        }
    }

    public static SelectedTab parseTab() {
        return IOUtils.loadResourceFromClasspath(TestDataXHiveReaderRealDump.class, NAME_TAB + "_schema.csv", true, (writerJsonInput) -> {
            SelectedTab tab = new SelectedTab();
            tab.name = NAME_TAB;
            List<String> lines = org.apache.commons.io.IOUtils.readLines(writerJsonInput, TisUTF8.get());
            for (String l : lines) {
                tab.cols.add(CMeta.create(StringUtils.substringBefore(l, ","), JDBCTypes.VARCHAR));
            }
            return tab;
        });
    }

    private static DataXHiveReader createReader(String dataXName) {
        DataXHiveReader hiveReader = new DataXHiveReader();
        SelectedTab tab = parseTab();
        hiveReader.selectedTabs = Collections.singletonList(tab);
        HiveDFSLinker hiveDFSLinker = new HiveDFSLinker();

        HdfsFileSystemFactory fsFactory = new HdfsFileSystemFactory();
        fsFactory.userHostname = true;
        fsFactory.rootDir = "/user/admin";
        fsFactory.userToken = UserTokenUtils.createNoneAuthToken();
        fsFactory.hdfsSiteContent //
                = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                + "\n"
                + "<configuration>\n"
                + "  <property>\n"
                + "    <name>fs.defaultFS</name>\n"
                + "    <value>hdfs://192.168.28.200:30070</value>\n"
                + "  </property>\n"
                + "</configuration>";
        // fsFactory.getFileSystem();


        hiveDFSLinker.fileSystem = fsFactory;
        hiveDFSLinker.linker = "hiveSourceRef";
        hiveReader.dfsLinker = hiveDFSLinker;
//        DefaultPartitionFilter partitionFilter = new DefaultPartitionFilter();
//        partitionFilter.ptFilter = DefaultPartitionFilter.getPtDftVal();
        hiveReader.ptFilter = NonePartition.create();// partitionFilter; //DataXHiveReader.getPtDftVal();
        return hiveReader;
    }
}
