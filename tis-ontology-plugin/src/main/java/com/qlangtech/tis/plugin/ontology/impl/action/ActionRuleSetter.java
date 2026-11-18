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
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.impl.action.type.ActionType;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.util.IPluginContext;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Action Type Step 2: 规则设置
 * <p>
 * 配置 Action 的执行规则。<b>一个 Action 可以组合多条规则</b>（Palantir:
 * "Action types can include combinations of Ontology rules."），列表顺序即执行顺序；
 * 非法组合由 {@link RuleCombinationValidator} 在提交时拦截。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/6
 * @see RuleCombinationValidator
 */
public class ActionRuleSetter extends OneStepOfMultiSteps {

    /**
     * 规则列表。用 {@code MULTI_DESCRIBLE_PLUGIN} 渲染成一张表，每行是一个
     * {@link ActionType} 子类实例。
     *
     * <p>注意<b>不要</b>再加 {@code @SubForm}：type-13 的嵌套形态完全由
     * {@code desClazz} 决定，{@code @SubForm} 是给「列表里放纯配置对象」用的另一套机制，
     * 两者叠加会让表单初始化走错分支。
     */
    @FormField(ordinal = 0, type = FormFieldType.MULTI_DESCRIBLE_PLUGIN,
            desClazz = ActionType.class, validate = {Validator.require})
    public List<ActionType> actionTypes;


    /**
     * 规则列表，永不为 null。
     */
    public List<ActionType> getActionTypes() {
        return this.actionTypes == null ? Collections.emptyList() : this.actionTypes;
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
            // 第二步完成后不再有下一步，直接结束向导
            return Optional.empty();
        }

        @Override
        public boolean isFinalStep() {
            return true;
        }

        @Override
        protected boolean validateAll(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
            // 规则之间的组合约束（对象操作先后顺序、Function 规则互斥等）无法用逐字段的
            // Validator 表达，只能在这里兜。
            ActionRuleSetter ruleSetter = postFormVals.newInstance();
            List<ActionType> rules = ruleSetter.getActionTypes();
            if (rules.isEmpty()) {
                msgHandler.addFieldError(context, "actionTypes", "至少需要配置一条规则");
                return false;
            }
            Optional<String> err = RuleCombinationValidator.validate(rules);
            if (err.isPresent()) {
                msgHandler.addFieldError(context, "actionTypes", err.get());
                return false;
            }
            return super.validateAll(msgHandler, context, postFormVals);
        }
    }
}
