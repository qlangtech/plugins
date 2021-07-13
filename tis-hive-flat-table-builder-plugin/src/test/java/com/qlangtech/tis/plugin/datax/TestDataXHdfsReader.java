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

import com.qlangtech.tis.datax.impl.DataxReader;
import com.qlangtech.tis.hdfs.impl.HdfsFileSystemFactory;
import com.qlangtech.tis.plugin.common.PluginDesc;
import com.qlangtech.tis.plugin.common.ReaderTemplate;
import com.qlangtech.tis.plugin.test.BasicTest;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-05-08 20:02
 **/
public class TestDataXHdfsReader extends BasicTest {

    public void testDescriptorsJSONGenerate() {
        PluginDesc.testDescGenerate(DataXHdfsReader.class, "hdfs-datax-reader-descriptor.json");
    }

    //    public void test() {
//        ContextDesc.descBuild(DataXHdfsReader.class, true);
//    }
    String dataXName = "testDataXName";

    public void testTemplateGenerate() throws Exception {


        DataXHdfsReader dataxReader = createHdfsReader(dataXName);


        ReaderTemplate.validateDataXReader("hdfs-datax-reader-assert.json", dataXName, dataxReader);


        dataxReader.compress = null;
        dataxReader.csvReaderConfig = null;
        dataxReader.fieldDelimiter = "\\t";


        ReaderTemplate.validateDataXReader("hdfs-datax-reader-assert-without-option-val.json", dataXName, dataxReader);
    }

    protected DataXHdfsReader createHdfsReader(String dataXName) {
        final HdfsFileSystemFactory fsFactory = TestDataXHdfsWriter.getHdfsFileSystemFactory();

        DataXHdfsReader dataxReader = new DataXHdfsReader() {
            @Override
            public HdfsFileSystemFactory getFs() {
                return fsFactory;
            }

            @Override
            public Class<?> getOwnerClass() {
                return DataXHdfsReader.class;
            }
        };
        dataxReader.dataXName = dataXName;
        dataxReader.template = DataXHdfsReader.getDftTemplate();
        dataxReader.column = "[\n" +
                "      {\n" +
                "        \"index\": 0,\n" +
                "        \"type\": \"string\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"index\": 1,\n" +
                "        \"type\": \"string\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"index\": 2,\n" +
                "        \"type\": \"string\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"index\": 3,\n" +
                "        \"type\": \"string\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"index\": 4,\n" +
                "        \"type\": \"string\"\n" +
                "      }\n" +
                "    ]";
        dataxReader.fsName = "default";
        dataxReader.compress = "gzip";
        dataxReader.csvReaderConfig = "{\n" +
                "        \"safetySwitch\": false,\n" +
                "        \"skipEmptyRecords\": false,\n" +
                "        \"useTextQualifier\": false\n" +
                "}";
        dataxReader.fieldDelimiter = ",";
        dataxReader.encoding = "utf-8";
        dataxReader.nullFormat = "\\\\N";
        dataxReader.fileType = "text";
        dataxReader.path = "tis/order/*";
        return dataxReader;
    }

    public void testRealDump() throws Exception {
        DataXHdfsReader dataxReader = createHdfsReader(dataXName);
        DataxReader.dataxReaderGetter = (name) -> {
            assertEquals(dataXName, name);
            return dataxReader;
        };

        ReaderTemplate.realExecute("hdfs-datax-reader-assert-without-option-val.json", dataxReader);
    }
}
