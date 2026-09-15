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

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.extension.OneStepOfMultiSteps;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.RuleGroup;
import com.qlangtech.tis.plugin.ontology.impl.action.type.ActionType;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.util.IPluginContext;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Action Type Step 2: 规则设置
 * <p>
 * 配置 Action 的执行规则（支持多个规则）
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/6
 */
public class ActionRuleSetter extends OneStepOfMultiSteps {

    @FormField(ordinal = 0, validate = {})
    public ActionType objectType;

    @FormField(ordinal = 1, validate = {})
    public ActionType linkType;

    @FormField(ordinal = 2, validate = {})
    public ActionType functionType;

//    @FormField(ordinal = 3, validate = {})
//    public ActionType interfaceType;

    @FormField(ordinal = 4, validate = {})
    public ActionType sideEffectType;

    @FormField(ordinal = 5, validate = {})
    public ActionType scheduleType;

    @FormField(ordinal = 6, validate = {})
    public ActionType applyScenarioType;

    /**
     * 获取所有已配置且启用的 ActionType 列表
     */
    public List<ActionType> getActionTypes() {
        return Stream.of(
                objectType, linkType, functionType,
                sideEffectType, scheduleType, applyScenarioType
        ).filter(Objects::nonNull).filter((t) -> t.getRuleGroup() != RuleGroup.OFF).toList();
    }

    @Override
    public void processPreSaved(IPluginContext pluginContext, Context currentCtx,
                                OneStepOfMultiSteps[] preSavedStepPlugins) {
        // 保存当前步骤数据到上下文
        currentCtx.put(ActionRuleSetter.class.getName(), this);
        super.processPreSaved(pluginContext, currentCtx, preSavedStepPlugins);
    }

    @TISExtension
    public static class Desc extends OneStepOfMultiSteps.BasicDesc {

        @Override
        public String getStepDescription() {
            return "Rules";
        }

        @Override
        public Step getStep() {
            return Step.Step2;
        }

        @Override
        public Optional<BasicDesc> nextPluginDesc(OneStepOfMultiSteps current) {
            // 指向下一步：ActionParameters
            return Optional.of(new ActionParameters.Desc());
        }

        @Override
        public boolean isFinalStep() {
            return false;
        }

        @Override
        protected boolean validateAll(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {

            ActionRuleSetter ruleSetter = postFormVals.newInstance();
            if (CollectionUtils.isEmpty(ruleSetter.getActionTypes())) {
                msgHandler.addErrorMessage(context, "至少选启用一项以上");
                return false;
            }

            return super.validateAll(msgHandler, context, postFormVals);
        }
//        @Override
//        public String getDisplayName() {
//            return "规则配置";
//        }
    }
}