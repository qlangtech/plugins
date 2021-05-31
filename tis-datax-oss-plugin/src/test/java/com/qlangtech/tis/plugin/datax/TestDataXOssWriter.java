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

import com.qlangtech.tis.datax.IDataxGlobalCfg;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.impl.DataXCfgGenerator;
import com.qlangtech.tis.extension.util.PluginExtraProps;
import com.qlangtech.tis.plugin.test.BasicTest;
import com.qlangtech.tis.trigger.util.JsonUtil;
import org.easymock.EasyMock;

import java.util.Optional;
import java.util.regex.Matcher;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-05-08 11:58
 **/
public class TestDataXOssWriter extends BasicTest {

    public void testFieldPattern() {
        Matcher matcher = DataXOssWriter.PATTERN_OSS_WRITER_OBJECT_NAME.matcher("test");
        assertTrue(matcher.matches());

        matcher = DataXOssWriter.PATTERN_OSS_WRITER_OBJECT_NAME.matcher("test/");
        assertFalse(matcher.matches());

        matcher = DataXOssWriter.PATTERN_OSS_WRITER_OBJECT_NAME.matcher("test/*");
        assertFalse(matcher.matches());

        matcher = DataXOssWriter.PATTERN_OSS_WRITER_OBJECT_NAME.matcher("test*");
        assertFalse(matcher.matches());
    }

    public void testPluginExtraPropsLoad() throws Exception {
        Optional<PluginExtraProps> extraProps = PluginExtraProps.load(DataXOssWriter.class);
        assertTrue(extraProps.isPresent());
    }

    public void testGetDftTemplate() {
        String dftTemplate = DataXOssWriter.getDftTemplate();
        assertNotNull("dftTemplate can not be null", dftTemplate);
    }

    public void testTempateGenerate() throws Exception {
        IDataxProcessor processor = EasyMock.mock("dataxProcessor", IDataxProcessor.class);

        IDataxGlobalCfg dataxGlobalCfg = EasyMock.mock("dataxGlobalCfg", IDataxGlobalCfg.class);
        EasyMock.expect(processor.getDataXGlobalCfg()).andReturn(dataxGlobalCfg).anyTimes();
        //IDataxWriter dataxWriter = EasyMock.mock("dataxWriter", IDataxWriter.class);

        //  IDataxContext dataxContext = EasyMock.mock("dataxWriterContext", IDataxContext.class);
        // EasyMock.expect(dataxWriter.getSubTask(Optional.empty())).andReturn(dataxContext).anyTimes();

        DataXOssWriter ossWriter = new DataXOssWriter();
        ossWriter.endpoint = "aliyun-bj-endpoint";
        ossWriter.bucket = "testBucket";
        ossWriter.object = "tis/mytable/*";
        ossWriter.template = DataXOssWriter.getDftTemplate();
        ossWriter.header = "[\"name\",\"age\",\"degree\"]";
        ossWriter.encoding = "utf-8";
        ossWriter.fieldDelimiter = "\t";
        ossWriter.writeMode = "nonConflict";
        ossWriter.dateFormat = "yyyy-MM-dd";
        ossWriter.fileFormat = "csv";
        ossWriter.maxFileSize = 300;
        ossWriter.nullFormat = "\\\\N";
        EasyMock.expect(processor.getWriter(null)).andReturn(ossWriter).anyTimes();
        EasyMock.replay(processor, dataxGlobalCfg);

        valiateWriterCfgGenerate("oss-datax-writer-assert.json", processor, ossWriter);

        ossWriter.fieldDelimiter = null;
        ossWriter.encoding = null;
        ossWriter.nullFormat = null;
        ossWriter.dateFormat = null;
        ossWriter.fileFormat = null;
        ossWriter.header = null;
        ossWriter.maxFileSize = null;
        valiateWriterCfgGenerate("oss-datax-writer-assert-without-option-val.json", processor, ossWriter);

        EasyMock.verify(processor, dataxGlobalCfg);
    }


    private void valiateWriterCfgGenerate(String assertFileName, IDataxProcessor processor, DataXOssWriter ossWriter) throws Exception {

        MockDataxReaderContext mockReaderContext = new MockDataxReaderContext();

        DataXCfgGenerator dataProcessor = new DataXCfgGenerator(null, testDataXName, processor) {
            @Override
            public String getTemplateContent() {
                return ossWriter.getTemplate();
            }
        };

        String readerCfg = dataProcessor.generateDataxConfig(mockReaderContext, Optional.empty());
        assertNotNull(readerCfg);
        System.out.println(readerCfg);
        JsonUtil.assertJSONEqual(this.getClass(), assertFileName, readerCfg);
    }
}
