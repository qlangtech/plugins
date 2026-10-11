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
import com.qlangtech.tis.util.IPluginContext;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Action Type Step 3: 参数配置
 * <p>
 * 定义 Action 的输入参数（全局参数池）。Parameters 是 ActionType 级别的全局变量，可以被多个 Rules 引用。
 *
 * <p>参数池由 {@link ActionParameterConfig} 结构化成子表单行，不再让用户手写 JSON 数组字符串。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/6
 * @see ActionParameterConfig
 */
public class ActionParameters extends OneStepOfMultiSteps {

    @FormField(ordinal = 0, type = FormFieldType.MULTI_DESCRIBLE_PLUGIN,
            desClazz = ActionParameterConfig.class, validate = {Validator.require})
    public List<ActionParameterConfig> parameters;

    /**
     * 供下游（例如提交条件的候选池）安全遍历，不必自己判空。
     */
    public List<ActionParameterConfig> getParameters() {
        return this.parameters == null ? Collections.emptyList() : this.parameters;
    }

    @Override
    public void processPreSaved(IPluginContext pluginContext, Context currentCtx,
                                OneStepOfMultiSteps[] preSavedStepPlugins) {
        // 保存当前步骤数据到上下文
        currentCtx.put(ActionParameters.class.getName(), this);

        // 本步骤保存时会顺带构建下一步（提交条件）的描述符，而那一刻上下文里只有本步骤，
        // 第 2 步（ActionRuleSetter）只是躺在 preSavedStepPlugins 数组里。
        // 提交条件的字段候选需要第 2 步选定的目标对象类型，所以这里把它也发布到上下文。
        // 与 ObjectTypePropertiesRelevant#processPreSaved 直接取 preSavedStepPlugins[Step2.getStepIndex()]
        // 是同一手法，区别只是这里放进上下文（下一步的描述符构建发生在本步骤返回之前）。
        final int step2Index = Step.Step2.getStepIndex();
        if (preSavedStepPlugins.length > step2Index && preSavedStepPlugins[step2Index] != null) {
            OneStepOfMultiSteps step2 = preSavedStepPlugins[step2Index];
            currentCtx.put(step2.getClass().getName(), step2);
        }

        super.processPreSaved(pluginContext, currentCtx, preSavedStepPlugins);
    }

    @TISExtension
    public static class Desc extends OneStepOfMultiSteps.BasicDesc {

        @Override
        public String getStepDescription() {
            return "Parameters";
        }

        @Override
        public Step getStep() {
            return Step.Step3;
        }

        @Override
        public Optional<BasicDesc> nextPluginDesc(OneStepOfMultiSteps current) {
            return Optional.of(new ActionSubmissionCriteria.Desc());
        }

        @Override
        public boolean isFinalStep() {
            return false;
        }
    }
}
