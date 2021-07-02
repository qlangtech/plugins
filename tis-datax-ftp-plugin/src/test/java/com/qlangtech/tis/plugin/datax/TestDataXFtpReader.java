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

import com.qlangtech.tis.extension.util.PluginExtraProps;
import com.qlangtech.tis.plugin.common.PluginDesc;
import com.qlangtech.tis.plugin.common.ReaderTemplate;
import junit.framework.TestCase;

import java.util.Optional;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-05-08 11:35
 **/
public class TestDataXFtpReader extends TestCase {
    public void testGetDftTemplate() {
        String dftTemplate = DataXFtpReader.getDftTemplate();
        assertNotNull("dftTemplate can not be null", dftTemplate);
    }

    public void testPluginExtraPropsLoad() throws Exception {
        Optional<PluginExtraProps> extraProps = PluginExtraProps.load(DataXFtpReader.class);
        assertTrue(extraProps.isPresent());
    }

    public void testDescGenerate() throws Exception {

        // ContextDesc.descBuild(DataXFtpReader.class, true);

        PluginDesc.testDescGenerate(DataXFtpReader.class, "ftp-datax-reader-descriptor.json");

    }

    public void testTemplateGenerate() throws Exception {

        String dataXName = "test";

        DataXFtpReader reader = new DataXFtpReader();

        reader.template = DataXFtpReader.getDftTemplate();
        reader.protocol = "ftp";
        reader.host = "192.168.28.201";
        reader.port = 21;
        reader.timeout = 59999;
        reader.connectPattern = "PASV";
        reader.username = "test";
        reader.password = "test";
        reader.path = "/home/hanfa.shf/ftpReaderTest/data";
        reader.column = " {\n" +
                "    \"type\": \"long\",\n" +
                "    \"index\": 0    \n" +
                " },\n" +
                " {\n" +
                "    \"type\": \"string\",\n" +
                "    \"value\": \"alibaba\"  \n" +
                " }";
        reader.fieldDelimiter = ",";
        reader.compress = "bzip2";
        reader.encoding = "utf-8";
        reader.skipHeader = true;
        reader.nullFormat = "\\\\N";
        reader.maxTraversalLevel = "99";
        reader.csvReaderConfig = "{\n" +
                "        \"safetySwitch\": false,\n" +
                "        \"skipEmptyRecords\": false,\n" +
                "        \"useTextQualifier\": false\n" +
                "}";

        ReaderTemplate.validateDataXReader("ftp-datax-reader-assert.json", dataXName, reader);


        reader.port = null;
        reader.timeout = null;
        reader.connectPattern = null;
        reader.compress = null;
        reader.encoding = null;
        reader.skipHeader = null;
        reader.nullFormat = null;
        reader.maxTraversalLevel = null;
        reader.csvReaderConfig = null;

        ReaderTemplate.validateDataXReader("ftp-datax-reader-assert-without-option-val.json", dataXName, reader);

    }
}
