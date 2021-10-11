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
