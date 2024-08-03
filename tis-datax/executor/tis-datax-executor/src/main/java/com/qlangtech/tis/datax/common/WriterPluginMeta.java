/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.qlangtech.tis.datax.common;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.util.container.JarLoader;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Sets;
import com.qlangtech.tis.datax.DataxExecutor;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.IDataxReader;
import com.qlangtech.tis.datax.IDataxReaderContext;
import com.qlangtech.tis.datax.IDataxWriter;
import com.qlangtech.tis.datax.impl.DataXCfgGenerator;
import com.qlangtech.tis.datax.impl.DataxProcessor;
import com.qlangtech.tis.plugin.datax.transformer.RecordTransformerRules;
import com.qlangtech.tis.util.IPluginContext;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.Optional;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-07-21 08:28
 **/
public class WriterPluginMeta {
    private final String pluginKey;
    private final String streamwriterClass;
    private final Configuration conf;

    public WriterPluginMeta(String pluginKey, String streamwriterClass, Configuration conf) {
        this.pluginKey = pluginKey;
        this.streamwriterClass = streamwriterClass;
        this.conf = conf;
    }

    public static void realExecute(final String dataXName, final Configuration readerCfg
                                   // , IDataXPluginMeta dataxReader
            , WriterPluginMeta writerPluginMeta
            , Optional<Pair<String, List<String>>> transformer, Optional<JarLoader> jarLoader) throws IllegalAccessException {
        realExecute(dataXName, writerPluginMeta, jarLoader).startPipeline(readerCfg, transformer, (jobContainer) -> {
        });

    }

    public static DataXRealExecutor realExecute(final String dataXName //, final Configuration readerCfg
                                                //, IDataXPluginMeta dataxReader
            , WriterPluginMeta writerPluginMeta, Optional<JarLoader> jarLoader) throws IllegalAccessException {
        IPluginContext pluginCtx = IPluginContext.namedContext(dataXName);
        IDataxProcessor dataxProcessor = DataxProcessor.load(pluginCtx, dataXName);
        DataXCfgGenerator dataXCfgGenerator = new DataXCfgGenerator(pluginCtx, dataXName, dataxProcessor) {
            @Override
            protected String getTemplateContent(IDataxReaderContext readerContext
                    , IDataxReader reader, IDataxWriter writer, RecordTransformerRules transformerRules) {
                return reader.getTemplate();
            }

            @Override
            public void validatePluginName(IDataxWriter writer, IDataxReader reader, JSONObject cfg) {
                // super.validatePluginName(writer, reader, cfg);
            }
        };
        IDataxReader reader = dataxProcessor.getReader(pluginCtx);

        final JarLoader uberClassLoader = jarLoader.orElseGet(() -> {
            return new JarLoader(new String[]{"."});
        });

        DataxExecutor.initializeClassLoader(
                Sets.newHashSet("plugin.reader." + reader.getDataxMeta().getName(), writerPluginMeta.getPluginKey())
                , uberClassLoader);

        return new DataXRealExecutor(dataXName, dataxProcessor, reader, writerPluginMeta, dataXCfgGenerator);
    }

    public String getPluginKey() {
        return this.pluginKey;
    }

    public String getStreamwriterClass() {
        return this.streamwriterClass;
    }

    public Configuration getConf() {
        return this.conf;
    }
}
