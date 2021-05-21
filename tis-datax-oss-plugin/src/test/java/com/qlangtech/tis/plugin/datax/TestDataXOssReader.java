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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.datax.*;
import com.qlangtech.tis.datax.impl.DataXCfgGenerator;
import com.qlangtech.tis.extension.util.PluginExtraProps;
import com.qlangtech.tis.manage.common.TisUTF8;
import com.qlangtech.tis.plugin.common.JsonUtils;
import com.qlangtech.tis.plugin.test.BasicTest;
import org.apache.commons.io.IOUtils;
import org.easymock.EasyMock;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Optional;
import java.util.regex.Matcher;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-05-08 11:35
 **/
public class TestDataXOssReader extends BasicTest {

    public void testFieldPattern() {
        Matcher matcher = DataXOssReader.PATTERN_OSS_OBJECT_NAME.matcher("bazhen/*");
        assertTrue(matcher.matches());

        matcher = DataXOssReader.PATTERN_OSS_OBJECT_NAME.matcher("bazhen");
        assertTrue(matcher.matches());

        matcher = DataXOssReader.PATTERN_OSS_OBJECT_NAME.matcher("bazhen/dddd123");
        assertTrue(matcher.matches());

        matcher = DataXOssReader.PATTERN_OSS_OBJECT_NAME.matcher("/bazhen123/dddd");
        assertFalse(matcher.matches());

        matcher = DataXOssReader.PATTERN_OSS_OBJECT_NAME.matcher("/bazhen123/dddd/");
        assertFalse(matcher.matches());

        matcher = DataXOssReader.pattern_oss_bucket.matcher("tisrelease");
        assertTrue(matcher.matches());

        matcher = DataXOssReader.pattern_oss_bucket.matcher("tis-release");
        assertTrue(matcher.matches());
    }

    public void testGetDftTemplate() {
        String dftTemplate = DataXOssReader.getDftTemplate();
        assertNotNull("dftTemplate can not be null", dftTemplate);
    }

    public void testPluginExtraPropsLoad() throws Exception {
        Optional<PluginExtraProps> extraProps = PluginExtraProps.load(DataXOssReader.class);
        assertTrue(extraProps.isPresent());
    }

    public void testTempateGenerate() throws Exception {

//        String tab = "\\t";
//
//        System.out.println("tab:" + StringEscapeUtils.unescapeJava(tab).length());

        IDataxProcessor processor = EasyMock.mock("dataxProcessor", IDataxProcessor.class);

        IDataxGlobalCfg dataxGlobalCfg = EasyMock.mock("dataxGlobalCfg", IDataxGlobalCfg.class);
        EasyMock.expect(processor.getDataXGlobalCfg()).andReturn(dataxGlobalCfg).anyTimes();
        IDataxWriter dataxWriter = EasyMock.mock("dataxWriter", IDataxWriter.class);
        EasyMock.expect(processor.getWriter()).andReturn(dataxWriter).anyTimes();
        IDataxContext dataxContext = EasyMock.mock("dataxWriterContext", IDataxContext.class);
        EasyMock.expect(dataxWriter.getSubTask(Optional.empty())).andReturn(dataxContext).anyTimes();

        DataXOssReader ossReader = new DataXOssReader();
        ossReader.endpoint = "aliyun-bj-endpoint";
        ossReader.bucket = "testBucket";
        ossReader.object = "tis/mytable/*";
        ossReader.template = DataXOssReader.getDftTemplate();
        ossReader.column = "[{type:\"string\",index:0},{type:\"string\",index:1},{type:\"string\",value:\"test\"}]";
        ossReader.encoding = "utf8";
        ossReader.fieldDelimiter = "\t";
        ossReader.compress = "zip";

        ossReader.nullFormat = "\\N";
        ossReader.skipHeader = true;
        ossReader.csvReaderConfig = "{\n" +
                "        \"safetySwitch\": false,\n" +
                "        \"skipEmptyRecords\": false,\n" +
                "        \"useTextQualifier\": false\n" +
                "}";


        EasyMock.replay(processor, dataxGlobalCfg, dataxWriter, dataxContext);

//        try (InputStream reader = this.getClass().getResourceAsStream("oss-datax-reader-assert.json")) {
//            JSONObject jsonObject = JSON.parseObject(IOUtils.toString(reader, TisUTF8.getName()));
//            System.out.println("nullFormat:" + jsonObject.getJSONObject("parameter").getString("nullFormat"));
//        }

        valiateReaderCfgGenerate("oss-datax-reader-assert.json", processor, ossReader);


        ossReader.encoding = null;
        ossReader.compress = null;
        ossReader.nullFormat = null;
        ossReader.skipHeader = null;
        ossReader.csvReaderConfig = "{}";
        valiateReaderCfgGenerate("oss-datax-reader-assert-without-option-val.json", processor, ossReader);

        EasyMock.verify(processor, dataxGlobalCfg, dataxWriter, dataxContext);
    }

    private void valiateReaderCfgGenerate(String assertFileName, IDataxProcessor processor, DataXOssReader ossReader) throws IOException {


        OSSReaderContext dataxReaderContext = null;
        Iterator<IDataxReaderContext> subTasks = ossReader.getSubTasks();
        int dataxReaderContextCount = 0;
        while (subTasks.hasNext()) {
            dataxReaderContext = (OSSReaderContext) subTasks.next();
            dataxReaderContextCount++;
        }
        assertEquals(1, dataxReaderContextCount);
        assertNotNull(dataxReaderContext);


        DataXCfgGenerator dataProcessor = new DataXCfgGenerator(processor) {
            @Override
            public String getTemplateContent() {
                return ossReader.getTemplate();
            }
        };

        String readerCfg = dataProcessor.generateDataxConfig(dataxReaderContext, Optional.empty());
        assertNotNull(readerCfg);
        System.out.println(readerCfg);
        JsonUtils.assertJSONEqual(this.getClass(), assertFileName, readerCfg);
    }
}
