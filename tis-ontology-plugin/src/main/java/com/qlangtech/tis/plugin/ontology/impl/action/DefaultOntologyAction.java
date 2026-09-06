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

package com.qlangtech.tis.plugin.ontology.impl.action;

import com.qlangtech.tis.extension.MultiStepsSupportHostDescriptor;
import com.qlangtech.tis.extension.OneStepOfMultiSteps;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.ontology.Ontology;
import com.qlangtech.tis.plugin.ontology.OntologyAction;
import com.qlangtech.tis.util.IPluginContext;

import java.util.List;

/**
 * 默认的 Ontology Action Type 实现
 *
 * 定义对象的操作类型，包括：
 * - 写操作（创建、修改、删除对象/链接）
 * - 副作用操作（通知、Webhook）
 * - 高级操作（函数支持、场景合并、数据构建触发）
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/6
 * @see ActionMetadata
 * @see ActionParameters
 * @see ActionRules
 */
public class DefaultOntologyAction extends OntologyAction {

    @Override
    public String identityValue() {
        // 从第一步获取名称作为标识
        return getName();
    }

    @Override
    public void manipuldateProcess(IPluginContext pluginContext,
                                   com.qlangtech.tis.util.UploadPluginMeta pluginMeta,
                                   java.util.Optional<com.alibaba.citrus.turbine.Context> context) {
        // 可以在这里添加自定义持久化逻辑
        // 例如：验证跨步骤的业务规则、生成默认值等
    }

    /**
     * 获取第一步（Metadata）
     */
    private ActionMetadata getMetadata() {
        return (ActionMetadata) getMultiStepsSavedItems()[0];
    }

    /**
     * 获取第二步（Parameters）
     */
    private ActionParameters getParameters() {
        return (ActionParameters) getMultiStepsSavedItems()[1];
    }

    /**
     * 获取第三步（Rules）
     */
    private ActionRules getRules() {
        return (ActionRules) getMultiStepsSavedItems()[2];
    }

    @Override
    public String getName() {
        return getMetadata().name;
    }

    @Override
    public String getDisplayName() {
        return getMetadata().displayName;
    }

    @Override
    public String getDescription() {
        return getMetadata().description;
    }

    /**
     * 获取目标对象类型
     */
    public String getTargetObjectType() {
        return getMetadata().targetObjectType;
    }

    /**
     * 获取参数配置（JSON）
     */
    public String getParametersConfig() {
        return getParameters().parameters;
    }

    /**
     * 获取规则类型
     */
    public String getRuleType() {
        return getRules().ruleType;
    }

    /**
     * 获取规则配置（JSON）
     */
    public String getRuleConfig() {
        return getRules().ruleConfig;
    }

    /**
     * 获取提交条件（JSON，可选）
     */
    public String getSubmissionCriteria() {
        return getRules().submissionCriteria;
    }

    @Override
    public java.util.List<com.qlangtech.tis.plugin.datax.transformer.UDFDesc> getLiteria() {
        java.util.List<com.qlangtech.tis.plugin.datax.transformer.UDFDesc> literia =
            com.google.common.collect.Lists.newArrayList();

        literia.add(new com.qlangtech.tis.plugin.datax.transformer.UDFDesc("Name", getName()));
        literia.add(new com.qlangtech.tis.plugin.datax.transformer.UDFDesc("Display Name", getDisplayName()));
        literia.add(new com.qlangtech.tis.plugin.datax.transformer.UDFDesc("Target Object Type", getTargetObjectType()));
        literia.add(new com.qlangtech.tis.plugin.datax.transformer.UDFDesc("Rule Type", getRuleType()));

        return literia;
    }

    @TISExtension
    public static class DefaultDesc extends Ontology.BasicDesc
            implements MultiStepsSupportHostDescriptor<OntologyAction> {

        @Override
        public Class<OntologyAction> getHostClass() {
            return OntologyAction.class;
        }

        @Override
        public List<OneStepOfMultiSteps.BasicDesc> getStepDescriptionList() {
            return List.of(
                new ActionMetadata.Desc(),
                new ActionParameters.Desc(),
                new ActionRules.Desc()
            );
        }

        @Override
        public void appendExternalProps(com.alibaba.fastjson.JSONObject multiStepsCfg) {
            // 可以在这里添加额外的配置信息到前端
        }

        @Override
        public OntologyEnum getOntologyType() {
            return OntologyEnum.Action;
        }

        @Override
        public String getDisplayName() {
            return "Action";
        }

        @Override
        public String shortComment() {
            return "定义本体对象的操作类型，包括写操作、通知、Webhook 等";
        }
    }
}
