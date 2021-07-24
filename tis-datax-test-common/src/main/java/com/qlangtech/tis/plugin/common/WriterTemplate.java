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

import com.alibaba.datax.common.element.ColumnCast;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.job.JobContainer;
import com.alibaba.datax.core.util.container.JarLoader;
import com.alibaba.datax.core.util.container.LoadUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Sets;
import com.qlangtech.tis.datax.*;
import com.qlangtech.tis.datax.impl.DataXCfgGenerator;
import com.qlangtech.tis.datax.impl.DataxWriter;
import com.qlangtech.tis.extension.impl.IOUtils;
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


//    public static final Field jarLoaderCenterField;
//
//    static {
//        try {
//            jarLoaderCenterField = LoadUtil.class.getDeclaredField("jarLoaderCenter");
//            jarLoaderCenterField.setAccessible(true);
//        } catch (NoSuchFieldException e) {
//            throw new RuntimeException("can not get field 'jarLoaderCenter' of LoadUtil", e);
//        }
//    }

    public static void valiateCfgGenerate(String assertFileName, DataxWriter dataXWriter, IDataxProcessor.TableMap tableMap) throws Exception {

        IDataxProcessor processor = EasyMock.mock("dataxProcessor", IDataxProcessor.class);

        IDataxGlobalCfg dataxGlobalCfg = EasyMock.mock("dataxGlobalCfg", IDataxGlobalCfg.class);
        EasyMock.expect(processor.getDataXGlobalCfg()).andReturn(dataxGlobalCfg).anyTimes();
        EasyMock.expect(processor.getWriter(null)).andReturn(dataXWriter);

        IDataxReader dataXReader = EasyMock.createMock("dataXReader", IDataxReader.class);

        EasyMock.expect(processor.getReader(null)).andReturn(dataXReader);

        MockDataxReaderContext mockReaderContext = new MockDataxReaderContext();
        EasyMock.replay(processor, dataxGlobalCfg, dataXReader);

        DataXCfgGenerator dataProcessor = new DataXCfgGenerator(null, BasicTest.testDataXName, processor) {
            @Override
            public String getTemplateContent() {
                return dataXWriter.getTemplate();
            }
        };

        String writerCfg = dataProcessor.generateDataxConfig(mockReaderContext, dataXWriter, dataXReader, Optional.ofNullable(tableMap));
        Assert.assertNotNull(writerCfg);
        System.out.println(writerCfg);
        JsonUtil.assertJSONEqual(dataXWriter.getClass(), assertFileName, writerCfg, (message, expected, actual) -> {
            Assert.assertEquals(message, expected, actual);
        });
        JSONObject writer = JSON.parseObject(writerCfg);

        Assert.assertEquals(dataXWriter.getDataxMeta().getName(), writer.getString("name"));

        EasyMock.verify(processor, dataxGlobalCfg, dataXReader);
    }

    /**
     * dataXWriter执行
     *
     * @param writerJson
     * @param dataxWriter
     * @throws IllegalAccessException
     */
    public static void realExecuteDump(final String writerJson, IDataXPluginMeta dataxWriter) throws IllegalAccessException {
        final JarLoader uberClassLoader = new JarLoader(new String[]{"."});
        DataxExecutor.initializeClassLoader(
                Sets.newHashSet("plugin.reader.streamreader", "plugin.writer." + dataxWriter.getDataxMeta().getName()), uberClassLoader);

//        Map<String, JarLoader> jarLoaderCenter = (Map<String, JarLoader>) jarLoaderCenterField.get(null);
//        jarLoaderCenter.clear();
//
//
//        jarLoaderCenter.put("plugin.reader.streamreader", uberClassLoader);
//        jarLoaderCenter.put("plugin.writer." + dataxWriter.getDataxMeta().getName(), uberClassLoader);

        Configuration allConf = IOUtils.loadResourceFromClasspath(MockDataxReaderContext.class //
                , "container.json", true, (input) -> {
                    Configuration cfg = Configuration.from(input);

//                    "streamreader": {
//                        "class": "com.alibaba.datax.plugin.reader.streamreader.StreamReader"
//                    }

                    cfg.set("plugin.reader.streamreader.class"
                            , "com.alibaba.datax.plugin.reader.streamreader.StreamReader");

                    cfg.set("plugin.writer." + dataxWriter.getDataxMeta().getName() + ".class"
                            , dataxWriter.getDataxMeta().getImplClass());
                    cfg.set("job.content[0].writer" //
                            , IOUtils.loadResourceFromClasspath(dataxWriter.getClass(), writerJson, true, (writerJsonInput) -> {
                                return Configuration.from(writerJsonInput);
                            }));

                    return cfg;
                });


        // 绑定column转换信息
        ColumnCast.bind(allConf);
        LoadUtil.bind(allConf);

        JobContainer container = new JobContainer(allConf);

        container.start();
    }
}
