/**
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.qlangtech.tis.plugin.datax;

import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.extension.util.PluginExtraProps;
import com.qlangtech.tis.plugin.common.PluginDesc;
import com.qlangtech.tis.plugin.common.WriterTemplate;
import com.qlangtech.tis.plugin.datax.test.TestSelectedTabs;
import junit.framework.TestCase;

import java.util.Optional;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-05-08 11:35
 **/
public class TestDataXFtpWriter extends TestCase {
    public void testGetDftTemplate() {
        String dftTemplate = DataXFtpWriter.getDftTemplate();
        assertNotNull("dftTemplate can not be null", dftTemplate);
    }

    public void testPluginExtraPropsLoad() throws Exception {
        Optional<PluginExtraProps> extraProps = PluginExtraProps.load(DataXFtpWriter.class);
        assertTrue(extraProps.isPresent());
    }

    public void testDescGenerate() {

        PluginDesc.testDescGenerate(DataXFtpWriter.class, "ftp-datax-writer-descriptor.json");
    }

    public void testTempateGenerate() throws Exception {

        //    ContextDesc.descBuild(DataXFtpWriter.class, false);

        DataXFtpWriter dataXWriter = new DataXFtpWriter();
        dataXWriter.template = DataXFtpWriter.getDftTemplate();
        dataXWriter.protocol = "ftp";
        dataXWriter.host = "192.168.28.201";
        dataXWriter.port = 21;
        dataXWriter.timeout = 33333;
        dataXWriter.username = "test";
        dataXWriter.password = "test";
        dataXWriter.path = "/tmp/data/";
       // dataXWriter.fileName = "yixiao";
        dataXWriter.writeMode = "truncate";
        dataXWriter.fieldDelimiter = ",";
        dataXWriter.encoding = "utf-8";
        dataXWriter.nullFormat = "\\\\N";
        dataXWriter.fileFormat = "text";
        dataXWriter.suffix = "xxxx";
        dataXWriter.header = true;

        IDataxProcessor.TableMap tableMap = TestSelectedTabs.createTableMapper().get();

        WriterTemplate.valiateCfgGenerate("ftp-datax-writer-assert.json", dataXWriter, tableMap);


        dataXWriter.port = null;
        dataXWriter.timeout = null;
        dataXWriter.fieldDelimiter = null;
        dataXWriter.encoding = null;
        dataXWriter.nullFormat = null;
        dataXWriter.dateFormat = null;
        dataXWriter.fileFormat = null;
        dataXWriter.suffix = null;
        dataXWriter.header = false;

        WriterTemplate.valiateCfgGenerate("ftp-datax-writer-assert-without-option-val.json", dataXWriter, tableMap);
    }


//    public void testRealDump() throws Exception {
//        DataXFtpWriter dataXWriter = new DataXFtpWriter();
//        WriterTemplate.realExecuteDump("ftp-datax-writer.json", dataXWriter);
//    }
}
