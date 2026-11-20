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

import com.google.common.collect.Lists;
import com.qlangtech.tis.datax.Delimiter;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.SourceColMetaGetter;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.extension.util.PluginExtraProps;
import com.qlangtech.tis.fs.ITISFileSystem;
import com.qlangtech.tis.hdfs.impl.HdfsPath;
import com.qlangtech.tis.hive.Hiveserver2DataSourceFactory;
import com.qlangtech.tis.offline.FileSystemFactory;
import com.qlangtech.tis.plugin.common.WriterTemplate;
import com.qlangtech.tis.plugin.datax.impl.TextFSFormat;
import com.qlangtech.tis.plugin.ds.CMeta;
import com.qlangtech.tis.plugin.ds.DataXReaderColType;
import com.qlangtech.tis.plugin.test.BasicTest;
import com.qlangtech.tis.trigger.util.JsonUtil;
import com.qlangtech.tis.util.DescriptorsJSON;
import com.qlangtech.tis.util.DescriptorsMeta;
import org.apache.commons.lang.StringUtils;
import org.mockito.Mockito;
import org.junit.Assert;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import static org.junit.Assert.*;
import org.junit.Test;
/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-05-27 15:17
 **/
public class TestDataXHiveWriter extends BasicTest {

    @Test
    public void testGetDftTemplate() {
        String dftTemplate = DataXHiveWriter.getDftTemplate();
        assertNotNull("dftTemplate can not be null", dftTemplate);
    }

    @Test
    public void testPluginExtraPropsLoad() throws Exception {
        Optional<PluginExtraProps> extraProps = PluginExtraProps.load(DataXHiveWriter.class);
        assertTrue(extraProps.isPresent());
    }

    @Test
    public void testDescriptorsJSONGenerate() {
        DataXHiveWriter writer = new DataXHiveWriter();
        DescriptorsJSON descJson = new DescriptorsJSON(writer.getDescriptor());

        DescriptorsMeta desc = descJson.getDescriptorsJSON();
        System.out.println(JsonUtil.toString(desc));

        JsonUtil.assertJSONEqual(TestDataXHiveWriter.class, "desc-json/datax-writer-hive.json", desc, (m, e, a) -> {
            assertEquals(m, e, a);
        });
    }

    final static String mysql2hiveDataXName = "mysql2hive";

    @Test
    public void testConfigGenerate() throws Exception {


        DataXHiveWriter hiveWriter = new DataXHiveWriter();
        hiveWriter.dataXName = mysql2hiveDataXName;
        hiveWriter.fsName = "hdfs1";
        TextFSFormat fsFormat = new TextFSFormat();
        fsFormat.fieldDelimiter = Delimiter.Tab.token;
        hiveWriter.fileType = fsFormat;// "text";
//        TabPrefixDecorator prefixDecorator = new TabPrefixDecorator();
//        prefixDecorator.prefix = "ods_";
//        hiveWriter.tabDecorator = prefixDecorator;// "ods_";
        hiveWriter.writeMode = "nonConflict";
        //  hiveWriter.fieldDelimiter = "\t";
        hiveWriter.compress = "gzip";
        hiveWriter.encoding = "utf-8";
        hiveWriter.template = DataXHiveWriter.getDftTemplate();
        hiveWriter.partitionRetainNum = 2;
        hiveWriter.partitionFormat = "yyyyMMdd";


        IDataxProcessor.TableMap tableMap = WriterTemplate.createCustomer_order_relationTableMap();


        WriterTemplate.valiateCfgGenerate("hive-datax-writer-assert.json", hiveWriter, tableMap);


        hiveWriter.compress = null;
        hiveWriter.encoding = null;

        WriterTemplate.valiateCfgGenerate("hive-datax-writer-assert-without-option-val.json", hiveWriter, tableMap);

    }

    @Test
    public void testGenerateCreateDDL() {

        FileSystemFactory fsFactory = Mockito.mock(FileSystemFactory.class, Mockito.withSettings().name("fsFactory"));


        ITISFileSystem fs = Mockito.mock(ITISFileSystem.class, Mockito.withSettings().name("fs"));
        Mockito.when(fs.getRootDir()).thenReturn(new HdfsPath("/user/admin"));
        Mockito.when(fsFactory.getFileSystem()).thenReturn(fs);

        DataXHiveWriter writer = new DataXHiveWriter() {
            @Override
            public Hiveserver2DataSourceFactory getDataSourceFactory() {
                return new Hiveserver2DataSourceFactory();
            }

            @Override
            public FileSystemFactory getFs() {
                return fsFactory;
            }
        };
        TextFSFormat txtFormat = new TextFSFormat();
        txtFormat.fieldDelimiter = Delimiter.Char001.token;
        writer.fileType = txtFormat;
        Assert.assertFalse(writer.isGenerateCreateDDLSwitchOff());
        CreateTableSqlBuilder.CreateDDL ddl = writer.generateCreateDDL(SourceColMetaGetter.getNone(), getApplicationTab(), Optional.empty());

        assertNotNull(ddl);

        assertEquals(
                StringUtils.trimToEmpty(IOUtils.loadResourceFromClasspath(DataXHiveWriter.class, "create-application-ddl.sql"))
                , ddl.getDDLScript());

        Mockito.verify(fsFactory).getFileSystem();
        Mockito.verify(fs).getRootDir();
    }

    public static IDataxProcessor.TableMap getApplicationTab() {
        return getTabApplication((cols) -> {
            CMeta col = new CMeta();
            col.setPk(true);
            col.setName("id3");
            col.setType(DataXReaderColType.Long.dataType);
            cols.add(col);

            col = new CMeta();
            col.setName("col4");
            col.setType(DataXReaderColType.STRING.dataType);
            cols.add(col);

            col = new CMeta();
            col.setName("col5");
            col.setType(DataXReaderColType.STRING.dataType);
            cols.add(col);


            col = new CMeta();
            col.setPk(true);
            col.setName("col6");
            col.setType(DataXReaderColType.STRING.dataType);
            cols.add(col);
        });
    }

    private static final String TAB_APPLICATION = "application";

    protected static IDataxProcessor.TableMap getTabApplication(
            Consumer<List<CMeta>>... colsProcess) {

        List<CMeta> sourceCols = Lists.newArrayList();
        CMeta col = new CMeta();
        col.setPk(true);
        col.setName("user_id");
        col.setType(DataXReaderColType.Long.dataType);
        sourceCols.add(col);

        col = new CMeta();
        col.setName("user_name");
        col.setType(DataXReaderColType.STRING.dataType);
        sourceCols.add(col);

        for (Consumer<List<CMeta>> p : colsProcess) {
            p.accept(sourceCols);
        }
        IDataxProcessor.TableMap tableMap = new IDataxProcessor.TableMap(Optional.of(TAB_APPLICATION), sourceCols);
        tableMap.setFrom(TAB_APPLICATION);
        tableMap.setTo(TAB_APPLICATION);
        //tableMap.setSourceCols(sourceCols);
        return tableMap;
    }


}
