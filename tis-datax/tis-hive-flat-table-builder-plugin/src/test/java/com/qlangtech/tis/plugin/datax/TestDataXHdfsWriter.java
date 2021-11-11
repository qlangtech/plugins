/**
 * Copyright (c) 2020 QingLang, Inc. <baisui@qlangtech.com>
 * <p>
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 * <p>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.qlangtech.tis.plugin.datax;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.datax.impl.DataxWriter;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.extension.util.PluginExtraProps;
import com.qlangtech.tis.fs.IPathInfo;
import com.qlangtech.tis.fs.ITISFileSystem;
import com.qlangtech.tis.fs.TISFSDataOutputStream;
import com.qlangtech.tis.hdfs.impl.HdfsFileSystemFactory;
import com.qlangtech.tis.hdfs.impl.HdfsPath;
import com.qlangtech.tis.manage.common.TisUTF8;
import com.qlangtech.tis.offline.FileSystemFactory;
import com.qlangtech.tis.plugin.common.WriterTemplate;
import com.qlangtech.tis.plugin.test.BasicTest;
import com.qlangtech.tis.trigger.util.JsonUtil;
import com.qlangtech.tis.util.DescriptorsJSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-05-08 11:35
 **/
public class TestDataXHdfsWriter extends BasicTest {
    private static final Logger logger = LoggerFactory.getLogger(TestDataXHdfsWriter.class);

    public void testGetDftTemplate() {
        String dftTemplate = DataXHdfsWriter.getDftTemplate();
        assertNotNull("dftTemplate can not be null", dftTemplate);
    }

    public void testPluginExtraPropsLoad() throws Exception {
        Optional<PluginExtraProps> extraProps = PluginExtraProps.load(DataXHdfsWriter.class);
        assertTrue(extraProps.isPresent());
    }

    public void testDescriptorsJSONGenerate() {
        DataXHdfsWriter writer = new DataXHdfsWriter();
        DescriptorsJSON descJson = new DescriptorsJSON(writer.getDescriptor());
        JSONObject desc = descJson.getDescriptorsJSON();
        System.out.println(JsonUtil.toString(desc));

        JsonUtil.assertJSONEqual(TestDataXHdfsWriter.class, "desc-json/datax-writer-hdfs.json", desc, (m, e, a) -> {
            assertEquals(m, e, a);
        });
    }

    private final String mysql2hdfsDataXName = "mysql2hdfs";
    private static final String hdfsRelativePath = "tis/order";

    public void testConfigGenerate() throws Exception {


        DataXHdfsWriter hdfsWriter = new DataXHdfsWriter();
        hdfsWriter.dataXName = mysql2hdfsDataXName;
        hdfsWriter.fsName = "hdfs1";
        hdfsWriter.fileType = "text";
        hdfsWriter.writeMode = "nonConflict";
        hdfsWriter.fieldDelimiter = "\t";
        hdfsWriter.compress = "gzip";
        hdfsWriter.encoding = "utf-8";
        hdfsWriter.template = DataXHdfsWriter.getDftTemplate();
        hdfsWriter.path = hdfsRelativePath;


        IDataxProcessor.TableMap tableMap = createCustomer_order_relationTableMap();


        WriterTemplate.valiateCfgGenerate("hdfs-datax-writer-assert.json", hdfsWriter, tableMap);


        hdfsWriter.compress = null;
        hdfsWriter.encoding = null;

        WriterTemplate.valiateCfgGenerate("hdfs-datax-writer-assert-without-option-val.json", hdfsWriter, tableMap);
    }

    //@Test
    public void testdataDump() throws Exception {

        //  final DataxWriter dataxWriter = DataxWriter.load(null, mysql2hdfsDataXName);


        HdfsFileSystemFactory fsFactory = getHdfsFileSystemFactory();
        ITISFileSystem fileSystem = fsFactory.getFileSystem();
//        assertNotNull("fileSystem can not be null", fileSystem);

//        new Path(fsFactory.rootDir
//                , this.cfg.getNecessaryValue(Key.PATH, HdfsWriterErrorCode.REQUIRED_VALUE));
//
//        fileSystem.getPath("");

        HdfsPath p = new HdfsPath(fsFactory.rootDir + "/tis/order");


        HdfsPath subWriterPath = new HdfsPath(p, "test");

        try (TISFSDataOutputStream outputStream = fileSystem.create(subWriterPath, true)) {
            org.apache.commons.io.IOUtils.write(IOUtils.loadResourceFromClasspath(DataXHdfsWriter.class
                    , "hdfs-datax-writer-assert-without-option-val.json"), outputStream, TisUTF8.get());
        }
        System.out.println("write file success");

        List<IPathInfo> iPathInfos = fileSystem.listChildren(p);
        for (IPathInfo child : iPathInfos) {
            fileSystem.delete(child.getPath(), true);
        }

        final DataXHdfsWriter hdfsWriter = new DataXHdfsWriter() {
            @Override
            public FileSystemFactory getFs() {
                return fsFactory;
            }

            @Override
            public Class<?> getOwnerClass() {
                return DataXHdfsWriter.class;
            }
        };


        DataxWriter.dataxWriterGetter = (name) -> {
            assertEquals("mysql2hdfs", name);
            return hdfsWriter;
        };

//        IPath path = fileSystem.getPath(fileSystem.getPath(fileSystem.getRootDir()), hdfsRelativePath);
//        System.out.println("clear path:" + path);
//        fileSystem.delete(path, true);
//
        WriterTemplate.realExecuteDump("hdfs-datax-writer-assert-without-option-val.json", hdfsWriter);
    }

    public static HdfsFileSystemFactory getHdfsFileSystemFactory() {
        HdfsFileSystemFactory fsFactory = new HdfsFileSystemFactory();
        fsFactory.setHdfsAddress("hdfs://daily-cdh201");
        fsFactory.setHdfsSiteContent(IOUtils.loadResourceFromClasspath(DataXHdfsWriter.class, "hdfs/hdfsSiteContent.xml"));
        fsFactory.rootDir = "/user/admin";
        return fsFactory;
    }

    public static IDataxProcessor.TableMap createCustomer_order_relationTableMap() {
        ISelectedTab.ColMeta colMeta = null;
        IDataxProcessor.TableMap tableMap = new IDataxProcessor.TableMap();
        tableMap.setTo("customer_order_relation");
        List<ISelectedTab.ColMeta> sourceCols = Lists.newArrayList();

        colMeta = new ISelectedTab.ColMeta();
        colMeta.setName("customerregister_id");
        colMeta.setType(ISelectedTab.DataXReaderColType.STRING);
        sourceCols.add(colMeta);

        colMeta = new ISelectedTab.ColMeta();
        colMeta.setName("waitingorder_id");
        colMeta.setType(ISelectedTab.DataXReaderColType.STRING);
        sourceCols.add(colMeta);

        colMeta = new ISelectedTab.ColMeta();
        colMeta.setName("kind");
        colMeta.setType(ISelectedTab.DataXReaderColType.INT);
        sourceCols.add(colMeta);

        colMeta = new ISelectedTab.ColMeta();
        colMeta.setName("create_time");
        colMeta.setType(ISelectedTab.DataXReaderColType.Long);
        sourceCols.add(colMeta);

        colMeta = new ISelectedTab.ColMeta();
        colMeta.setName("last_ver");
        colMeta.setType(ISelectedTab.DataXReaderColType.INT);
        sourceCols.add(colMeta);

        tableMap.setSourceCols(sourceCols);
        return tableMap;
    }


}
