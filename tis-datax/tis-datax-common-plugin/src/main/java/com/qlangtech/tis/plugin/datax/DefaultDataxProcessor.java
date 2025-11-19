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

package com.qlangtech.tis.plugin.datax;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.config.ParamsConfig;
import com.qlangtech.tis.datax.DBDataXChildTask;
import com.qlangtech.tis.datax.DataXName;
import com.qlangtech.tis.datax.DefaultDataXProcessorManipulate;
import com.qlangtech.tis.datax.IDataxGlobalCfg;
import com.qlangtech.tis.datax.IDataxReader;
import com.qlangtech.tis.datax.StoreResourceType;
import com.qlangtech.tis.datax.StoreResourceTypeConstants;
import com.qlangtech.tis.datax.impl.DataxProcessor;
import com.qlangtech.tis.datax.impl.TransformerInfo;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.IDescribableManipulate;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.manage.IAppSource;
import com.qlangtech.tis.manage.biz.dal.pojo.AppType;
import com.qlangtech.tis.manage.biz.dal.pojo.Application;
import com.qlangtech.tis.manage.common.AppAndRuntime;
import com.qlangtech.tis.plugin.IEndTypeGetter;
import com.qlangtech.tis.plugin.IPluginStore;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.transformer.RecordTransformerRules;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.incr.TISSinkFactory;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import com.qlangtech.tis.sql.parser.tuple.creator.IStreamIncrGenerateStrategy;
import com.qlangtech.tis.util.IPluginContext;
import com.qlangtech.tis.util.UploadPluginMeta;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.Function;

import static com.qlangtech.tis.plugin.datax.DataFlowDataXProcessor.addTransformerInfo;

/**
 * @author: baisui 百岁
 * @create: 2021-04-21 09:09
 * @see DataFlowDataXProcessor
 **/
public class DefaultDataxProcessor extends DataxProcessor {

    public static final String KEY_FIELD_NAME = "globalCfg";

    @FormField(identity = true, ordinal = 0, validate = {Validator.require, Validator.identity, Validator.forbid_start_with_number})
    public String name;

    @FormField(ordinal = 1, type = FormFieldType.SELECTABLE, validate = {Validator.require})
    public String globalCfg;

    @FormField(ordinal = 2, type = FormFieldType.ENUM, validate = {Validator.require})
    public String dptId;
    @FormField(ordinal = 3, validate = {Validator.require})
    public String recept;

    @Override
    public StoreResourceType getResType() {
        return StoreResourceType.DataApp;
    }

    @Override
    public Application buildApp() {
        Application app = new Application();
        app.setProjectName(this.name);
        app.setDptId(Integer.parseInt(this.dptId));
        app.setRecept(this.recept);
        app.setUpdateTime(new Date());
        app.setLastProcessTime(new Date());

        app.setAppType(AppType.DataXPipe.getType());
        return app;
    }

    @Override
    public IDataxReader getReader(IPluginContext pluginContext, ISelectedTab tab) {
        return this.getReader(pluginContext);
    }

    @Override
    public Pair<List<RecordTransformerRules>, IPluginStore> getRecordTransformerRulesAndPluginStore(IPluginContext pluginCtx, String tableName) {
        if (StringUtils.isEmpty(tableName)) {
            throw new IllegalArgumentException("param tableName:" + tableName + " can not be empty");
        }
        return DataFlowDataXProcessor.loadRecordTransformerRulesAndPluginStore(pluginCtx, this.getResType(), this.name, tableName);
    }

    /**
     * @param pluginCtx
     * @param groupedChildTask key: tableName
     * @return
     */
    @Override
    public Set<TransformerInfo> getTransformerInfo(IPluginContext pluginCtx, Map<String, List<DBDataXChildTask>> groupedChildTask) {
        Set<TransformerInfo> tinfos = new HashSet<>();
        addTransformerInfo(tinfos, pluginCtx, groupedChildTask, this.getResType(), this.identityValue(), (tableName, context) -> {
            Pair<List<RecordTransformerRules>, IPluginStore> tabTransformerRule
                    = DataFlowDataXProcessor.loadRecordTransformerRulesAndPluginStore(context, this.getResType(), this.name, tableName);
            for (RecordTransformerRules trule : tabTransformerRule.getKey()) {
                return Optional.of(trule);
            }
            return Optional.empty();
        });
        return tinfos;
    }


    @Override
    public void copy(String newIdentityVal) {
        if (StringUtils.isEmpty(newIdentityVal)) {
            throw new IllegalArgumentException("param newIdentityVal can not be empty");
        }
        try {
            File workDir = this.getDataXWorkDir(null);
            if (!workDir.exists()) {
                throw new IllegalStateException("workDir:" + workDir.getAbsolutePath() + " is not exist ");
            }
            File newWorkDir = new File(workDir.getParentFile(), newIdentityVal);
            FileUtils.copyDirectory(workDir, newWorkDir);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String identityValue() {
        return this.name;
    }

    public IDataxGlobalCfg getDataXGlobalCfg() {
        IDataxGlobalCfg globalCfg = ParamsConfig.getItem(this.globalCfg, IDataxGlobalCfg.KEY_DISPLAY_NAME);
        Objects.requireNonNull(globalCfg, "dataX Global config can not be null");
        return globalCfg;
    }

    @Override
    public IStreamTemplateResource getFlinkStreamGenerateTplResource() {

        return writerPluginOverwrite((d) -> d.getFlinkStreamGenerateTplResource(),
                () -> DefaultDataxProcessor.super.getFlinkStreamGenerateTplResource());
    }

    @Override
    public IStreamTemplateData decorateMergeData(IStreamTemplateData mergeData) {
        return writerPluginOverwrite((d) -> d.decorateMergeData(mergeData), () -> mergeData);
    }

    private <T> T writerPluginOverwrite(Function<IStreamIncrGenerateStrategy, T> func, Callable<T> unmatchCreator) {
        try {
            TISSinkFactory sinKFactory = TISSinkFactory.getIncrSinKFactory(this.getDataXName());
            Objects.requireNonNull(sinKFactory, "writer plugin can not be null");
            if (sinKFactory instanceof IStreamIncrGenerateStrategy) {
                return func.apply(((IStreamIncrGenerateStrategy) sinKFactory));
            }
            return unmatchCreator.call();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    @TISExtension()
    public static class DescriptorImpl extends Descriptor<IAppSource> implements IDescribableManipulate<DefaultDataXProcessorManipulate>, IEndTypeGetter {
        static final int MAX_RECEPT_LENGTH = 20;

        public DescriptorImpl() {
            super();
            this.registerSelectOptions(KEY_FIELD_NAME, () -> ParamsConfig.getItems(IDataxGlobalCfg.KEY_DISPLAY_NAME));
        }

        @Override
        public EndType getEndType() {
            return EndType.Pipeline;
        }

        public boolean validateRecept(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {

            if (StringUtils.length(value) > MAX_RECEPT_LENGTH) {
                msgHandler.addFieldError(context, fieldName, "长度不能超过" + MAX_RECEPT_LENGTH + "个字符");
                return false;
            }
            return true;
        }

        public boolean validateName(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            UploadPluginMeta pluginMeta = (UploadPluginMeta) context.get(UploadPluginMeta.KEY_PLUGIN_META);
            Objects.requireNonNull(pluginMeta, "pluginMeta can not be null");
            if (pluginMeta.isUpdate()) {
                return true;
            }
            return msgHandler.validateBizLogic(IFieldErrorHandler.BizLogic.VALIDATE_APP_NAME_DUPLICATE, context, fieldName,
                    value);
        }

        @Override
        public String getDisplayName() {
            return StoreResourceTypeConstants.DEFAULT_DATAX_PROCESSOR_NAME;
        }

        @Override
        public Class<DefaultDataXProcessorManipulate> getManipulateExtendPoint() {
            return DefaultDataXProcessorManipulate.class;
        }

        @Override
        public Optional<IPluginStore<DefaultDataXProcessorManipulate>> getManipulateStore() {

            AppAndRuntime appAndRuntime = AppAndRuntime.getAppAndRuntime();
            DataXName appName = Objects.requireNonNull(appAndRuntime, "appAndRuntime can not be null").getAppName();
            if (appName == null) {
                return Optional.empty();
            }

            return Optional.of(DefaultDataXProcessorManipulate.getPluginStore(null, appName));

            //  return Optional.of(DefaultDataXProcessorManipulate.loadPlugins(null, DefaultDataXProcessorManipulate.class, appName).getValue());
        }
    }


}
