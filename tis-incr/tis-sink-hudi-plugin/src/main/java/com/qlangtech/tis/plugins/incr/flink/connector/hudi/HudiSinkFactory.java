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

package com.qlangtech.tis.plugins.incr.flink.connector.hudi;

import com.alibaba.citrus.turbine.Context;
import com.alibaba.datax.common.util.Configuration;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.qlangtech.tis.compiler.incr.ICompileAndPackage;
import com.qlangtech.tis.compiler.streamcode.CompileAndPackage;
import com.qlangtech.tis.datax.IDataXPluginMeta;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.IDataxReader;
import com.qlangtech.tis.datax.IStreamTableCreator;
import com.qlangtech.tis.datax.impl.DataxProcessor;
import com.qlangtech.tis.datax.impl.DataxWriter;
import com.qlangtech.tis.extension.PluginWrapper;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.util.GroovyShellEvaluate;
import com.qlangtech.tis.manage.common.Config;
import com.qlangtech.tis.manage.common.Option;
import com.qlangtech.tis.order.center.IParamContext;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.hudi.DataXHudiWriter;
import com.qlangtech.tis.plugin.datax.hudi.HudiSelectedTab;
import com.qlangtech.tis.plugin.datax.hudi.HudiTableMeta;
import com.qlangtech.tis.plugin.incr.TISSinkFactory;
import com.qlangtech.tis.plugins.incr.flink.connector.hudi.streamscript.BasicFlinkStreamScriptCreator;
import com.qlangtech.tis.realtime.transfer.DTO;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.annotation.Public;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.hudi.common.fs.IExtraHadoopFileSystemGetter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-02-14 14:39
 **/
@Public
public class HudiSinkFactory extends TISSinkFactory implements IStreamTableCreator {
    public static final String DISPLAY_NAME_FLINK_CDC_SINK = "Flink-Hudi-Sink";
    public static final String HIVE_SYNC_MODE = "hms";

    private static final Logger logger = LoggerFactory.getLogger(HudiSinkFactory.class);

    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.db_col_name})
    public String catalog;

    @FormField(ordinal = 2, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.db_col_name})
    public String database;

    @FormField(ordinal = 3, type = FormFieldType.ENUM, validate = {Validator.require})
    public String dumpTimeStamp;

    @FormField(ordinal = 4, type = FormFieldType.ENUM, validate = {Validator.require})
    public String scriptType;

    @FormField(ordinal = 5, type = FormFieldType.INT_NUMBER, validate = {Validator.require, Validator.integer})
    public Integer currentLimit;

    private transient IStreamTableCreator streamTableCreator;

    private IStreamTableCreator getStreamTableCreator() {
        if (streamTableCreator != null) {
            return streamTableCreator;
        }
        return streamTableCreator = BasicFlinkStreamScriptCreator.createStreamTableCreator(this);
    }


    public static List<Option> getHistoryBatch() {

        HudiSinkFactory sink = (HudiSinkFactory) GroovyShellEvaluate.pluginThreadLocal.get();
        if (sink != null) {
            try {
                DataXHudiWriter dataXWriter = getDataXHudiWriter(sink);
                return HudiTableMeta.getHistoryBatchs(dataXWriter.getFs().getFileSystem(), dataXWriter.getHiveConnMeta());
            } catch (Exception e) {
                logger.warn(e.getMessage(), e);
            }
        }

        return Lists.newArrayList(new Option(IParamContext.getCurrentTimeStamp()));

    }

    public static DataXHudiWriter getDataXHudiWriter(HudiSinkFactory sink) {
        return (DataXHudiWriter) DataxWriter.load(null, sink.dataXName);
    }

    @Override
    public Map<IDataxProcessor.TableAlias, SinkFunction<DTO>> createSinkFunction(IDataxProcessor dataxProcessor) {
        DataXHudiWriter hudiWriter = getDataXHudiWriter(this);

        if (!IExtraHadoopFileSystemGetter.HUDI_FILESYSTEM_NAME.equals(hudiWriter.fsName)) {
            throw new IllegalStateException("fsName of hudiWriter must be equal to '"
                    + IExtraHadoopFileSystemGetter.HUDI_FILESYSTEM_NAME + "', but now is " + hudiWriter.fsName);
        }

        return Collections.emptyMap();
    }

    public String getDataXName() {
        if (StringUtils.isEmpty(this.dataXName)) {
            throw new IllegalStateException("prop dataXName can not be null");
        }
        return this.dataXName;
    }

    private transient Map<String, Pair<HudiSelectedTab, HudiTableMeta>> tableMetas = null;


    public Pair<HudiSelectedTab, HudiTableMeta> getTableMeta(String tableName) {
        if (StringUtils.isEmpty(tableName)) {
            throw new IllegalArgumentException("param tableName can not empty");
        }
        if (tableMetas == null) {
            if (StringUtils.isEmpty(this.dataXName)) {
                throw new IllegalStateException("prop dataXName can not be null");
            }
            tableMetas = Maps.newHashMap();
            DataxProcessor dataXProcessor = DataxProcessor.load(null, this.dataXName);

            IDataxReader reader = dataXProcessor.getReader(null);
            Map<String, HudiSelectedTab> selTabs
                    = reader.getSelectedTabs().stream()
                    .map((tab) -> (HudiSelectedTab) tab).collect(Collectors.toMap((tab) -> tab.getName(), (tab) -> tab));

            List<File> dataxCfgFile = dataXProcessor.getDataxCfgFileNames(null);
            Configuration cfg = null;
            Configuration paramCfg = null;
            String table = null;
            HudiTableMeta tableMeta = null;
            for (File f : dataxCfgFile) {
                cfg = Configuration.from(f);
                paramCfg = cfg.getConfiguration("job.content[0].writer.parameter");
                if (paramCfg == null) {
                    throw new NullPointerException("paramCfg can not be null,relevant path:" + f.getAbsolutePath());
                }
                table = paramCfg.getString("fileName");
                if (StringUtils.isEmpty(table)) {
                    throw new IllegalStateException("table can not be null:" + paramCfg.toJSON());
                }

                tableMetas.put(table, Pair.of(Objects.requireNonNull(selTabs.get(table), "tab:" + table + " relevant 'HudiSelectedTab' can not be null")
                        , new HudiTableMeta(paramCfg)));
            }
        }

        final Pair<HudiSelectedTab, HudiTableMeta> tabMeta = tableMetas.get(tableName);
        if (tabMeta == null || tabMeta.getRight().isColsEmpty()) {
            throw new IllegalStateException("table:" + tableName
                    + " relevant colMetas can not be null,exist tables:"
                    + tableMetas.keySet().stream().collect(Collectors.joining(",")));
        }
        return tabMeta;
    }

    /**
     * ------------------------------------------------------------------------------
     * start implements IStreamTableCreator
     * ------------------------------------------------------------------------------
     */

    @Override
    public IStreamTableMeta getStreamTableMeta(final String tableName) {
        return getStreamTableCreator().getStreamTableMeta(tableName);
    }

    @Override
    public String getFlinkStreamGenerateTemplateFileName() {
        return getStreamTableCreator().getFlinkStreamGenerateTemplateFileName();
    }

    @Override
    public IStreamTemplateData decorateMergeData(IStreamTemplateData mergeData) {
        return getStreamTableCreator().decorateMergeData(mergeData);
    }


    @Override
    public ICompileAndPackage getCompileAndPackageManager() {

        PluginWrapper.Dependency();

        return new CompileAndPackage(Lists.newArrayList(
                Config.getPluginLibDir("tis-sink-hudi-plugin").getAbsolutePath() + "/*"
                , Config.getPluginLibDir("tis-datax-hudi-plugin").getAbsolutePath() + "/*"));
    }


    /**
     * ------------------------------------------------------------------------------
     * End implements IStreamTableCreator
     * ------------------------------------------------------------------------------
     */

    @TISExtension
    public static class DefaultSinkFunctionDescriptor extends BaseSinkFunctionDescriptor {
        @Override
        public String getDisplayName() {
            return DISPLAY_NAME_FLINK_CDC_SINK;
        }

        @Override
        protected boolean validateAll(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
            if (!DataXHudiWriter.HUDI_FILESYSTEM_NAME.equals(IExtraHadoopFileSystemGetter.HUDI_FILESYSTEM_NAME)) {
                throw new IllegalStateException("DataXHudiWriter.HUDI_FILESYSTEM_NAME:" + DataXHudiWriter.HUDI_FILESYSTEM_NAME
                        + ",IExtraHadoopFileSystemGetter.HUDI_FILESYSTEM_NAME:"
                        + IExtraHadoopFileSystemGetter.HUDI_FILESYSTEM_NAME + " must be equal");
            }
            return super.validateAll(msgHandler, context, postFormVals);
        }

        @Override
        protected IDataXPluginMeta.EndType getTargetType() {
            return IDataXPluginMeta.EndType.Hudi;
        }
    }
}
