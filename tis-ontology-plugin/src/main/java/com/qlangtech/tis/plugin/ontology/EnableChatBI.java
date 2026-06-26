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
package com.qlangtech.tis.plugin.ontology;

import com.alibaba.citrus.turbine.Context;
import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.aiagent.llm.LLMProvider;
import com.qlangtech.tis.datax.IManipulateStatus;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.lang.PayloadLink;
import com.qlangtech.tis.manage.common.Config;
import com.qlangtech.tis.plugin.IPluginStore;
import com.qlangtech.tis.plugin.IdentityDesc;
import com.qlangtech.tis.plugin.IdentityName;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ds.manipulate.ManipulateItemsProcessor;
import com.qlangtech.tis.plugin.ds.manipulate.ManipuldateUtils;
import com.qlangtech.tis.plugin.manipulate.ManipulatePluginCacheRegister;
import com.qlangtech.tis.plugin.ontology.chatbi.ChatBIResult;
import com.qlangtech.tis.plugin.ontology.chatbi.ChatBIService;
import com.qlangtech.tis.plugin.ontology.chatbi.DefaultChatBIService;
import com.qlangtech.tis.plugin.ontology.chatbi.TraceStep;
import com.qlangtech.tis.plugin.ontology.chatbi.config.ValidationConfig;
import com.qlangtech.tis.plugin.ontology.impl.OntologyPluginMeta;
import com.qlangtech.tis.plugin.ontology.sync.OntologyNeo4jSyncService;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.util.IPluginContext;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

import static com.qlangtech.tis.manage.common.UserProfile.KEY_FIELD_LLM_NAME;

/**
 * 开启智能问数，design/chat-bi/06-neo4j-ontology-sync.md
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/5/28
 * @see DefaultChatBIService
 */
public class EnableChatBI extends OntologyDomainManipulate implements ChatBIService, IManipulateStatus //
        , IdentityDesc<JSONObject>, IPluginStore.BeforePluginSaved, IPluginStore.AfterPluginSaved {

    public static final String KEY_ID_NAME = "chat_bi";
    private String ontologyDomain;
    private transient ChatBIService _chatBIService;

    public static EnableChatBI load(String ontologyDomain) {
        ManipulatePluginCacheRegister.TemplateManipulateStore<OntologyDomainManipulate> manipulateStore = getManipulateStore(ontologyDomain, false);
        return manipulateStore.getManipuldate(IdentityName.create(KEY_ID_NAME), EnableChatBI.class);
    }

    /**
     * 大模型接口
     */
    @FormField(type = FormFieldType.SELECTABLE, ordinal = 1, validate = {Validator.identity, Validator.require})
    public String llm;

    /**
     * 检索配置
     */
    @FormField(ordinal = 3, validate = {Validator.require})
    public com.qlangtech.tis.plugin.ontology.chatbi.config.RetrievalConfig retrievalConfig;

    /**
     * 重试配置
     */
    @FormField(ordinal = 4, validate = {Validator.require})
    public com.qlangtech.tis.plugin.ontology.chatbi.config.RetryConfig retryConfig;

    /**
     * 校验配置
     */
    @FormField(ordinal = 5, validate = {Validator.require})
    public com.qlangtech.tis.plugin.ontology.chatbi.config.ValidationConfig validationConfig;

    /**
     * 执行配置
     */
    @FormField(ordinal = 6, validate = {Validator.require})
    public com.qlangtech.tis.plugin.ontology.chatbi.config.ExecutionConfig executionConfig;

    /**
     * Trace 配置
     */
    @FormField(ordinal = 7, validate = {Validator.require})
    public com.qlangtech.tis.plugin.ontology.chatbi.config.TraceConfig traceConfig;

    @Override
    public void afterSaved(IPluginContext pluginContext, Optional<Context> context) {
        _chatBIService = null;
    }

    /**
     * 获取当前域对应的 ChatBIService 实例（懒加载，每个 EnableChatBI 实例独立持有）。
     */
    private ChatBIService getChatBIService() {
        if (_chatBIService == null) {
            DefaultChatBIService svc = new DefaultChatBIService();
            svc.setLlmProvider(LLMProvider.load(
                    IPluginContext.namedContext(this.ontologyDomain).setLoginUser(() -> Config.ADMIN_NAME), this.llm));
            svc.setConfigs(
                    retryConfig != null ? retryConfig : createDefaultRetryConfig(),
                    validationConfig != null ? validationConfig : createDefaultValidationConfig(),
                    executionConfig != null ? executionConfig : createDefaultExecutionConfig(),
                    retrievalConfig != null ? retrievalConfig : createDefaultRetrievalConfig()
            );
            _chatBIService = svc;
        }
        return _chatBIService;
    }

    @Override
    public ChatBIResult ask(String domain, String nlq, boolean forceQueryExecute, Consumer<TraceStep> stepCallback) {
        return getChatBIService().ask(domain, nlq, forceQueryExecute, stepCallback);
    }

    @Override
    public void initialize() {
        OntologyNeo4jSyncService.getInstance();
    }

    @Override
    protected void afterManipuldateProcess(IPluginContext pluginContext, Optional<Context> context,
                                           ManipulateItemsProcessor itemsProcessor) {
        super.afterManipuldateProcess(pluginContext, context, itemsProcessor);
        if (itemsProcessor.isDeleteProcess()) {
            return;
        }

        // 初始化 Trace 清理服务
//        com.qlangtech.tis.plugin.ontology.chatbi.trace.TraceCleanupService.getInstance()
//                .setConfig(this.traceConfig != null ? this.traceConfig : createDefaultTraceConfig());

        OntologyPluginMeta meta = OntologyPluginMeta.createPluginMeta(itemsProcessor.getPluginMeta());
        //OntologySyncQueue.enqueue(() -> {
        IPluginContext.setPluginContext(pluginContext);
        OntologyNeo4jSyncService.getInstance().fullRebuild(meta.getDomain());
        //});
    }

    private com.qlangtech.tis.plugin.ontology.chatbi.config.RetryConfig createDefaultRetryConfig() {
        com.qlangtech.tis.plugin.ontology.chatbi.config.RetryConfig config =
                new com.qlangtech.tis.plugin.ontology.chatbi.config.RetryConfig();
        config.maxRetry = 2;
        config.explainTimeout = Duration.ofSeconds(5);
        return config;
    }

    private com.qlangtech.tis.plugin.ontology.chatbi.config.ValidationConfig createDefaultValidationConfig() {
        com.qlangtech.tis.plugin.ontology.chatbi.config.ValidationConfig config =
                new com.qlangtech.tis.plugin.ontology.chatbi.config.ValidationConfig();
        config.enableExplain = true;
        config.enableKeywordCheck = true;
        config.enableAstCheck = true;
        config.allowedFirstKeywords = ValidationConfig.dftAllowedFirstKeywords();
        config.forbiddenKeywords = ValidationConfig.dftForbiddenKeywords();
        config.safeFunctions = ValidationConfig.dftSafeFunctions();
        return config;
    }

    private com.qlangtech.tis.plugin.ontology.chatbi.config.ExecutionConfig createDefaultExecutionConfig() {
        com.qlangtech.tis.plugin.ontology.chatbi.config.ExecutionConfig config =
                new com.qlangtech.tis.plugin.ontology.chatbi.config.ExecutionConfig();
        config.executeQuery = true;
        config.maxResultRows = 200;
        config.queryTimeout = Duration.ofSeconds(30);
        return config;
    }

    private com.qlangtech.tis.plugin.ontology.chatbi.config.RetrievalConfig createDefaultRetrievalConfig() {
        com.qlangtech.tis.plugin.ontology.chatbi.config.RetrievalConfig config =
                new com.qlangtech.tis.plugin.ontology.chatbi.config.RetrievalConfig();
        config.topKSeeds = 5;
        config.maxHops = 2;
        config.tokenBudget = 3000;
        config.includeValueExamples = false;
        return config;
    }

    private com.qlangtech.tis.plugin.ontology.chatbi.config.TraceConfig createDefaultTraceConfig() {
        com.qlangtech.tis.plugin.ontology.chatbi.config.TraceConfig config =
                new com.qlangtech.tis.plugin.ontology.chatbi.config.TraceConfig();
        config.maxTracesPerDomain = 1000;
        config.retentionDays = 7;
        config.enableAutoCleanup = true;
        return config;
    }

    @Override
    public JSONObject describePlugin() {
        return Descriptor.getManipulateMeta(false, this);
    }

    @Override
    public ManipulateStateSummary manipulateStatusSummary() {
        final StringBuilder summary = new StringBuilder("已经开启ChatBI功能");
        return new ManipulateStateSummary(
                Collections.singletonList(IManipulateStatus.create("正常"))
                , summary.toString(), true);
    }

    @Override
    public Optional<PayloadLink> manipulateManagerPath() {
        return Optional.of(new PayloadLink("查看状态", "/base/ontology/" + this.ontologyDomain + "/chat-bi"));
    }

    @Override
    public void beforeSaved(IPluginContext pluginContext, Optional<Context> context) {
        OntologyPluginMeta pluginMeta = OntologyPluginMeta.createPluginMeta();
        this.ontologyDomain = Objects.requireNonNull(pluginMeta, "pluginMeta can not be null").getDomain();
    }

    @TISExtension
    public static final class DftDesc extends OntologyDomainManipulate.BasicDesc implements DescriptorUseableShortComment {
        public DftDesc() {
            super();
            this.registerSelectOptions(KEY_FIELD_LLM_NAME, LLMProvider::getExistProviders);
        }

        @Override
        public boolean isManipulateStorable() {
            return true;
        }

        @Override
        public EndType getEndType() {
            return EndType.ChatBI;
        }

        @Override
        protected boolean validateAll(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
            ManipulateItemsProcessor itemProcess //
                    = ManipuldateUtils.instance((IPluginContext) msgHandler, context, null, (m) -> {
            });
            OntologyPluginMeta pluginMeta = OntologyPluginMeta.createPluginMeta(itemProcess.getPluginMeta());
            List<OntologyObjectType> objectTypes = OntologyObjectType.loadAll(pluginMeta.getDomain());
            List<String> objTypes = objectTypes.stream() //
                    .filter((ot) -> !ot.getDataSourceBinding().hasBound()) //
                    .map((ot) -> "\"" + ot.getName() + "\"").toList();
            if (!objTypes.isEmpty()) {
                msgHandler.addErrorMessage(context, String.valueOf(Ontology.OntologyEnum.ObjectType) + ":" + String.join(",", objTypes) + "还未绑定数据源");
                return false;
            }
            return true;
        }

        @Override
        public String getDisplayName() {
            return "Enable ChatBI";
        }

        @Override
        public String shortComment() {
            return "开启自然语言统计问数功能";
        }
    }
}
