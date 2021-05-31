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

import com.qlangtech.tis.datax.IDataxGlobalCfg;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.impl.DataXCfgGenerator;
import com.qlangtech.tis.datax.impl.DataxWriter;
import com.qlangtech.tis.plugin.datax.MockDataxReaderContext;
import com.qlangtech.tis.plugin.test.BasicTest;
import com.qlangtech.tis.trigger.util.JsonUtil;
import org.easymock.EasyMock;
import org.junit.Assert;

import java.util.Optional;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-05-29 16:28
 **/
public class WriterTemplate {


    public static void valiateCfgGenerate(String assertFileName, DataxWriter dataXWriter, IDataxProcessor.TableMap tableMap) throws Exception {

        IDataxProcessor processor = EasyMock.mock("dataxProcessor", IDataxProcessor.class);

        IDataxGlobalCfg dataxGlobalCfg = EasyMock.mock("dataxGlobalCfg", IDataxGlobalCfg.class);
        EasyMock.expect(processor.getDataXGlobalCfg()).andReturn(dataxGlobalCfg).anyTimes();
        EasyMock.expect(processor.getWriter(null)).andReturn(dataXWriter);

        MockDataxReaderContext mockReaderContext = new MockDataxReaderContext();
        EasyMock.replay(processor, dataxGlobalCfg);

        DataXCfgGenerator dataProcessor = new DataXCfgGenerator(null, BasicTest.testDataXName, processor) {
            @Override
            public String getTemplateContent() {
                return dataXWriter.getTemplate();
            }
        };


        String writerCfg = dataProcessor.generateDataxConfig(mockReaderContext, Optional.of(tableMap));
        Assert.assertNotNull(writerCfg);
        System.out.println(writerCfg);
        JsonUtil.assertJSONEqual(dataXWriter.getClass(), assertFileName, writerCfg, (message, expected, actual) -> {
            Assert.assertEquals(message, expected, actual);
        });

        EasyMock.verify(processor, dataxGlobalCfg);
    }

}
