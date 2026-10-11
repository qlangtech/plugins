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
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Action Step 4: 提交条件。
 *
 * <p>取代原先的 {@code ActionRules}——那是一个「规则类型 + 规则配置 JSON + 提交条件 JSON」
 * 三字段的自由文本步骤，与 Step2 的 {@code ActionRuleSetter} 功能重复，且没有任何消费方。
 * 现在这一步只承担一件事：定义 Action 何时可以提交。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/11
 * @see SubmissionCriterion
 */
public class ActionSubmissionCriteria extends OneStepOfMultiSteps {

    /**
     * 提交条件列表。刻意可为空：零条件 == 不限制提交。
     */
    @FormField(ordinal = 0, type = FormFieldType.MULTI_DESCRIBLE_PLUGIN,
            desClazz = SubmissionCriterion.class, validate = {})
    public List<SubmissionCriterion> criteria;

    public List<SubmissionCriterion> getCriteria() {
        return this.criteria == null ? Collections.emptyList() : this.criteria;
    }

    // 本步骤是末步，不覆写 processPreSaved：上下文里没有任何下游消费者。

    @TISExtension
    public static class Desc extends OneStepOfMultiSteps.BasicDesc {

        @Override
        public String getStepDescription() {
            return "Submission Criteria";
        }

        @Override
        public Step getStep() {
            return Step.Step4;
        }

        @Override
        public Optional<BasicDesc> nextPluginDesc(OneStepOfMultiSteps current) {
            // 末步
            return Optional.empty();
        }

        @Override
        public boolean isFinalStep() {
            return true;
        }

        @Override
        protected boolean validateAll(IControlMsgHandler msgHandler, Context context, PostFormVals vals) {
            // 「一元操作符不取值、二元操作符必须取值」是行内两个字段之间的约束，
            // 逐字段的 validateXxx 表达不了，只能在这里兜。
            ActionSubmissionCriteria step = vals.newInstance();
            for (SubmissionCriterion c : step.getCriteria()) {
                if (c.operator != null && c.operator.isNullary() && StringUtils.isNotEmpty(c.value)) {
                    msgHandler.addErrorMessage(context, "条件「" + c.label + "」：「" + c.operator
                            + "」不接受比较值，请清空比较值");
                    return false;
                }
            }
            return super.validateAll(msgHandler, context, vals);
        }
    }
}
