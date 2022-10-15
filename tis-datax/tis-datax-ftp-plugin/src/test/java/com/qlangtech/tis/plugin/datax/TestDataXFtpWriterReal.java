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

package com.qlangtech.tis.plugin.datax;

import com.alibaba.datax.plugin.writer.hdfswriter.HdfsColMeta;
import com.google.common.collect.Lists;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.impl.DataxProcessor;
import com.qlangtech.tis.datax.impl.DataxWriter;
import com.qlangtech.tis.manage.common.TisUTF8;
import com.qlangtech.tis.plugin.common.WriterJson;
import com.qlangtech.tis.plugin.common.WriterTemplate;
import com.qlangtech.tis.plugin.ds.DataType;
import org.apache.commons.io.FileUtils;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.sql.Types;
import java.util.List;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-10-15 11:54
 **/
public class TestDataXFtpWriterReal {
    static FTPContainer ftpContainer;

    public static void initialize() {
        ftpContainer = new FTPContainer();
        ftpContainer.start();
    }

    @Test
    public void testStubWrite() {

    }


    @Test
    public void testRealDump() throws Exception {

        final String targetTableName = "customer_order_relation";
        String testDataXName = "mysql_ftp";

        final DataXFtpWriter writer = getFTPWriter();
        // writer.dataXName = testDataXName;
        List<HdfsColMeta> colMetas = Lists.newArrayList();

//                "customerregister_id",
//                "waitingorder_id",
//                "kind",
//                "create_time",
//                "last_ver"
        // DataType
        HdfsColMeta cmeta = null;
        // String colName, Boolean nullable, Boolean pk, DataType dataType
        cmeta = new HdfsColMeta("customerregister_id", false
                , true, new DataType(Types.VARCHAR, "VARCHAR", 150));
        colMetas.add(cmeta);

        cmeta = new HdfsColMeta("waitingorder_id", false, true
                , new DataType(Types.VARCHAR, "VARCHAR", 150));
        colMetas.add(cmeta);

        cmeta = new HdfsColMeta("kind"
                , true, false, new DataType(Types.BIGINT));
        colMetas.add(cmeta);

        cmeta = new HdfsColMeta("create_time"
                , true, false, new DataType(Types.BIGINT));
        colMetas.add(cmeta);

        cmeta = new HdfsColMeta("last_ver"
                , true, false, new DataType(Types.BIGINT));
        colMetas.add(cmeta);

        IDataxProcessor.TableMap tabMap = IDataxProcessor.TableMap.create(targetTableName, colMetas);
//        CreateTableSqlBuilder.CreateDDL ddl = writer.generateCreateDDL(tabMap);

        WriterJson wjson = WriterJson.content(WriterTemplate.cfgGenerate(writer, tabMap));

        WriterTemplate.realExecuteDump(wjson, writer);

//        CreateStarRocksWriter createDorisWriter = new CreateStarRocksWriter().invoke();
//        createDorisWriter.dsFactory.password = "";
//        // createDorisWriter.dsFactory.nodeDesc = "192.168.28.201";
//        createDorisWriter.dsFactory.nodeDesc = "localhost";
//
//        createDorisWriter.writer.autoCreateTable = true;

//        DataxProcessor dataXProcessor = EasyMock.mock("dataXProcessor", DataxProcessor.class);
//        File createDDLDir = new File(".");
//        File createDDLFile = null;
//        try {
//            createDDLFile = new File(createDDLDir, targetTableName + IDataxProcessor.DATAX_CREATE_DDL_FILE_NAME_SUFFIX);
//            FileUtils.write(createDDLFile, ddl.getDDLScript(), TisUTF8.get());
//
//            EasyMock.expect(dataXProcessor.getDataxCreateDDLDir(null)).andReturn(createDDLDir);
//            DataxWriter.dataxWriterGetter = (dataXName) -> {
//                return writer;
//            };
//            DataxProcessor.processorGetter = (dataXName) -> {
//                Assert.assertEquals(testDataXName, dataXName);
//                return dataXProcessor;
//            };
//            EasyMock.replay(dataXProcessor);
//            String[] jdbcUrl = new String[1];
//
//            WriterTemplate.realExecuteDump("oracle_writer_real_dump.json", writer, (cfg) -> {
//                cfg.set("parameter.connection[0].jdbcUrl", jdbcUrl[0]);
//                return cfg;
//            });
//
//            EasyMock.verify(dataXProcessor);
//        } finally {
//            FileUtils.deleteQuietly(createDDLFile);
//        }
    }

    private DataXFtpWriter getFTPWriter() {
        return new DataXFtpWriter();
    }
}
