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

package com.qlangtech.tis.plugin.common;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.qlangtech.tis.datax.*;
import com.qlangtech.tis.datax.impl.DataXCfgGenerator;
import com.qlangtech.tis.datax.impl.DataxReader;
import com.qlangtech.tis.plugin.ds.ColumnMetaData;
import junit.framework.TestCase;
import org.easymock.EasyMock;
import org.junit.Assert;

import java.io.IOException;
import java.sql.Types;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-05-23 17:06
 **/
public class ReaderTemplate {

    public static void validateDataXReader(String assertFileName, String dataXName, DataxReader dataxReader) throws IOException {
        IDataxProcessor processor = EasyMock.mock("dataxProcessor", IDataxProcessor.class);

        IDataxGlobalCfg dataxGlobalCfg = EasyMock.mock("dataxGlobalCfg", IDataxGlobalCfg.class);
        EasyMock.expect(processor.getDataXGlobalCfg()).andReturn(dataxGlobalCfg).anyTimes();
        IDataxWriter dataxWriter = EasyMock.mock("dataxWriter", IDataxWriter.class);
        EasyMock.expect(processor.getWriter(null)).andReturn(dataxWriter).anyTimes();
        IDataxContext dataxContext = EasyMock.mock("dataxWriterContext", IDataxContext.class);
        EasyMock.expect(dataxWriter.getSubTask(Optional.empty())).andReturn(dataxContext).anyTimes();

        EasyMock.expect(processor.getReader(null)).andReturn(dataxReader);

        EasyMock.replay(processor, dataxGlobalCfg, dataxWriter, dataxContext);

//        try (InputStream reader = this.getClass().getResourceAsStream("oss-datax-reader-assert.json")) {
//            JSONObject jsonObject = JSON.parseObject(IOUtils.toString(reader, TisUTF8.getName()));
//            System.out.println("nullFormat:" + jsonObject.getJSONObject("parameter").getString("nullFormat"));
//        }

        valiateReaderCfgGenerate(assertFileName, processor, dataxReader, dataXName);


        EasyMock.verify(processor, dataxGlobalCfg, dataxWriter, dataxContext);
    }

    private static void valiateReaderCfgGenerate(String assertFileName, IDataxProcessor processor
            , DataxReader dataXReader, String dataXName) throws IOException {


        IDataxReaderContext dataxReaderContext = null;
        Iterator<IDataxReaderContext> subTasks = dataXReader.getSubTasks();
        int dataxReaderContextCount = 0;
        while (subTasks.hasNext()) {
            dataxReaderContext = subTasks.next();
            dataxReaderContextCount++;
        }
        TestCase.assertEquals(1, dataxReaderContextCount);
        TestCase.assertNotNull(dataxReaderContext);


        DataXCfgGenerator dataProcessor = new DataXCfgGenerator(null, dataXName, processor) {
            @Override
            public String getTemplateContent() {
                return dataXReader.getTemplate();
            }
        };

        String readerCfg = dataProcessor.generateDataxConfig(dataxReaderContext, Optional.empty());
        TestCase.assertNotNull(readerCfg);
        System.out.println(readerCfg);
        com.qlangtech.tis.trigger.util.JsonUtil.assertJSONEqual(dataXReader.getClass(), assertFileName, readerCfg, (msg, expect, actual) -> {
            Assert.assertEquals(msg, expect, actual);
        });
        JSONObject reader = JSON.parseObject(readerCfg);
        Assert.assertEquals(dataXReader.getDataxMeta().getName(), reader.getString("name"));
    }

}
