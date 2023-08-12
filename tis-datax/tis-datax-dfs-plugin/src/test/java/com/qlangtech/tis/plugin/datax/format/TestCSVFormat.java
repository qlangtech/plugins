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

package com.qlangtech.tis.plugin.datax.format;

import com.qlangtech.tis.datax.Delimiter;
import com.qlangtech.tis.extension.impl.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-08-09 22:34
 **/
public class TestCSVFormat extends BasicFormatTest {

    @Test
    public void testCreateWriter() throws Exception {
        CSVFormat txtFormat = new CSVFormat();
        txtFormat.header = true;
        txtFormat.fieldDelimiter = Delimiter.Comma.token;
        txtFormat.dateFormat = "yyyy-MM-dd";
        txtFormat.nullFormat = "null";
        txtFormat.csvReaderConfig = "{\"forceQualifier\":true}";


        Assert.assertEquals(IOUtils.loadResourceFromClasspath(TestTextFormat.class, "tbl_writer_header_true_delimiter_comma.csv")
                , testWithInsert(txtFormat));

        txtFormat.header = false;
        Assert.assertEquals(IOUtils.loadResourceFromClasspath(TestTextFormat.class, "tbl_writer_header_false_delimiter_comma.csv")
                , testWithInsert(txtFormat));

//        txtFormat.header = false;
//        txtFormat.fieldDelimiter = Delimiter.Comma.token;
//        Assert.assertEquals(IOUtils.loadResourceFromClasspath(TestTextFormat.class, "tbl_writer_header_false_delimiter_comma.text"), testWithInsert(txtFormat));
//
//
//        txtFormat.fieldDelimiter = Delimiter.Char001.token;
//        Assert.assertEquals(
//                StringUtils.replace(
//                        IOUtils.loadResourceFromClasspath(TestTextFormat.class, "tbl_writer_header_false_delimiter_comma.text")
//                        , String.valueOf(Delimiter.Comma.val)
//                        , String.valueOf(Delimiter.Char001.val))
//                , testWithInsert(txtFormat));
//        DefaultRecord record = new DefaultRecord();
//        record.
//
//                txtWrite.writeOneRecord();
    }
}
