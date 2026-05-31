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
import com.qlangtech.tis.plugin.IdentityDesc;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ds.manipulate.ManipulateItemsProcessor;
import com.qlangtech.tis.plugin.ontology.impl.OntologyPluginMeta;
import com.qlangtech.tis.plugin.ontology.sync.OntologyNeo4jSyncService;
import com.qlangtech.tis.plugin.ontology.sync.OntologySyncQueue;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.util.IPluginContext;

import java.util.Collections;
import java.util.Optional;

import static com.qlangtech.tis.manage.common.UserProfile.KEY_FIELD_LLM_NAME;

/**
 * 开启智能问数，design/chat-bi/06-neo4j-ontology-sync.md
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/5/28
 */
public class EnableChatBI extends OntologyDomainManipulate implements IManipulateStatus, IdentityDesc<JSONObject> {

    /**
     * 大模型接口
     */
    @FormField(type = FormFieldType.SELECTABLE, ordinal = 1, validate = {Validator.identity, Validator.require})
    public String llm;

    /**
     * chatBI 最大检索记录条数，放置获取记录数目太多导致信息过载
     */
    @FormField(type = FormFieldType.INT_NUMBER, ordinal = 2, validate = {Validator.integer, Validator.require})
    public Integer maxRetrvalCount;

    @Override
    protected void afterManipuldateProcess(IPluginContext pluginContext, Optional<Context> context,
                                           ManipulateItemsProcessor itemsProcessor) {
        super.afterManipuldateProcess(pluginContext, context, itemsProcessor);
        if (itemsProcessor.isDeleteProcess()) {
            return;
        }
        OntologyPluginMeta meta = OntologyPluginMeta.createPluginMeta(itemsProcessor.getPluginMeta());
        OntologySyncQueue.enqueue(() -> {
            IPluginContext.setPluginContext(pluginContext);
            OntologyNeo4jSyncService.getInstance().fullRebuild(meta.getDomain());
        });
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