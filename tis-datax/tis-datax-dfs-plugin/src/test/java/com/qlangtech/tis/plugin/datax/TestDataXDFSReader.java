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

import com.alibaba.datax.plugin.unstructuredstorage.reader.UnstructuredStorageReaderUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.qlangtech.tis.datax.IDataxProcessor.TableMap;
import com.qlangtech.tis.extension.util.PluginExtraProps;
import com.qlangtech.tis.manage.common.TisUTF8;
import com.qlangtech.tis.plugin.common.PluginDesc;
import com.qlangtech.tis.plugin.common.ReaderTemplate;
import com.qlangtech.tis.plugin.datax.meta.DefaultMetaDataWriter;
import com.qlangtech.tis.plugin.datax.resmatcher.MetaAwareDFSResMatcher;
import com.qlangtech.tis.plugin.datax.resmatcher.TestMetaAwareDFSResMatcher.MockTDFSLinker;
import com.qlangtech.tis.plugin.datax.server.FTPServer;
import com.qlangtech.tis.plugin.datax.tdfs.impl.FtpTDFSLinker;
import com.qlangtech.tis.plugin.ds.CMeta;
import com.qlangtech.tis.plugin.ds.ColumnMetaData;
import com.qlangtech.tis.plugin.ds.DataType;
import com.qlangtech.tis.selectedtab.TabApplicationCreator;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;
import com.qlangtech.tis.trigger.util.JsonUtil;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-05-08 11:35
 **/
public class TestDataXDFSReader {
    @Test
    public void testGetDftTemplate() {
        String dftTemplate = DataXDFSReader.getDftTemplate();
        Assert.assertNotNull("dftTemplate can not be null", dftTemplate);
    }

    @Test
    public void testPluginExtraPropsLoad() throws Exception {
        Optional<PluginExtraProps> extraProps = PluginExtraProps.load(AbstractDFSReader.class);
        Assert.assertTrue(extraProps.isPresent());
    }

    @Test
    public void testConvertMeta() throws Exception {

        File meta = new File("/Users/mozhenghua/Downloads/meta.json");

        // JSONArray types = JSON.parseArray(FileUtils.readFileToString(meta, TisUTF8.get()));

        List<ColumnMetaData> types = DefaultMetaDataWriter.deserialize(JSON.parseArray(FileUtils.readFileToString(meta, TisUTF8.get())));


        JSONArray convertTypes = new JSONArray();
        JSONObject convertType = null;
        int index = 0;
        for (ColumnMetaData type : types) {
            convertType = new JSONObject();
            convertType.put("type", map2Type(type.getType()).name());
            convertType.put("index", index++);
            convertTypes.add(convertType);
        }

        System.out.println(JsonUtil.toString(convertTypes));
    }

    private UnstructuredStorageReaderUtil.Type map2Type(DataType type) {
        return type.accept(new DataType.TypeVisitor<UnstructuredStorageReaderUtil.Type>() {
            @Override
            public UnstructuredStorageReaderUtil.Type bigInt(DataType type) {
                return UnstructuredStorageReaderUtil.Type.LONG;
            }

            @Override
            public UnstructuredStorageReaderUtil.Type doubleType(DataType type) {
                return UnstructuredStorageReaderUtil.Type.DOUBLE;
            }

            @Override
            public UnstructuredStorageReaderUtil.Type dateType(DataType type) {
                return UnstructuredStorageReaderUtil.Type.DATE;
            }

            @Override
            public UnstructuredStorageReaderUtil.Type timestampType(DataType type) {
                return UnstructuredStorageReaderUtil.Type.DATE;
            }

            @Override
            public UnstructuredStorageReaderUtil.Type bitType(DataType type) {
                return UnstructuredStorageReaderUtil.Type.BOOLEAN;
            }

            @Override
            public UnstructuredStorageReaderUtil.Type blobType(DataType type) {
                return UnstructuredStorageReaderUtil.Type.STRING;
            }

            @Override
            public UnstructuredStorageReaderUtil.Type varcharType(DataType type) {
                return UnstructuredStorageReaderUtil.Type.STRING;
            }
        });
    }

    @Test
    public void testDescGenerate() throws Exception {

        // ContextDesc.descBuild(DataXFtpReader.class, true);

        PluginDesc.testDescGenerate(AbstractDFSReader.class, "ftp-datax-reader-descriptor.json");

    }

    @Test
    public void testTemplateGenerate() throws Exception {

        String dataXName = "test";
        final String tabName = "orderdetail";
        // final FTPServer ftpServer = FtpWriterUtils.createFtpServer(null);
        DataXDFSReader reader = new DataXDFSReader();
        MockTDFSLinker ftpLinker = new MockTDFSLinker("/user/admin");
        // ftpLinker.path = "/admin/tis";
        reader.dfsLinker = ftpLinker;
        MetaAwareDFSResMatcher resMatcher = new MetaAwareDFSResMatcher();
        List<ColumnMetaData> cols = resMatcher.getTableMetadata(reader, TableMap.create(tabName, Lists.newArrayList()));
        SelectedTab tab = new SelectedTab();


        tab.cols.addAll(cols.stream().map((c) -> {
            CMeta meta = new CMeta();
            meta.setName(c.getName());
            meta.setType(c.getType());
            return meta;
        }).collect(Collectors.toList()));
        // SelectedTab tab = (SelectedTab) TabApplicationCreator.getTabApplication().getSourceTab();
        tab.name = tabName;
        reader.selectedTabs = Lists.newArrayList(tab);

//        reader.compress = Compress.noCompress.token;
        reader.template = DataXDFSReader.getDftTemplate();


        reader.resMatcher = resMatcher;
        // reader.linker = FtpWriterUtils.createFtpServer();
//        reader.protocol = "ftp";
//        reader.host = "192.168.28.201";
//        reader.port = 21;
//        reader.timeout = 59999;
//        reader.connectPattern = "PASV";
//        reader.username = "test";
//        reader.password = "test";
        // reader.path = "/home/hanfa.shf/ftpReaderTest/data";
//        reader.column = " [{\n" +
//                "    \"type\": \"long\",\n" +
//                "    \"index\": 0    \n" +
//                " },\n" +
//                " {\n" +
//                "    \"type\": \"string\",\n" +
//                "    \"value\": \"alibaba\"  \n" +
//                " }]";

        reader.fileFormat = FtpWriterUtils.createCsvFormat();

        // reader.fieldDelimiter = ",";

        // reader.skipHeader = true;
//        reader.nullFormat = "\\\\N";
//        reader.maxTraversalLevel = 99;
//        reader.csvReaderConfig = "{\n" +
//                "        \"safetySwitch\": false,\n" +
//                "        \"skipEmptyRecords\": false,\n" +
//                "        \"useTextQualifier\": false\n" +
//                "}";

        ReaderTemplate.validateDataXReader("ftp-datax-reader-assert.json", dataXName, reader);


//        FTPServer   ftpServers = FtpWriterUtils.createFtpServer(null);
//        ftpServers.port = null;
//        ftpServers.timeout = null;
//        ftpServers.connectPattern = null;
        // reader.linker = ftpServer;
        reader.fileFormat = FtpWriterUtils.createTextFormat();
//        reader.port = null;
//        reader.timeout = null;
//        reader.connectPattern = null;

        // reader.skipHeader = null;
//        reader.nullFormat = null;
//        reader.maxTraversalLevel = null;
        //reader.csvReaderConfig = null;
        // reader.fileFormat

        ReaderTemplate.validateDataXReader("ftp-datax-reader-assert-without-option-val.json", dataXName, reader);

    }
}
