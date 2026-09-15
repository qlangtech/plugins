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

package com.qlangtech.tis.plugin.ontology.impl.action.type;

import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.OntologyActionRule;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.RuleGroup;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.RuleType;

import java.util.List;

/**
 * Side Effect Action Type - 副作用操作
 * <p>
 * 对应 Side Effect 操作，包括：
 * - NOTIFICATION: 发送通知
 * - WEBHOOK: 调用外部 API（编辑后执行）
 * - WRITEBACK_WEBHOOK: 编辑前调用外部 API（最多一个）
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/7
 */
public class SideEffectActionType extends ActionType {

    @FormField(ordinal = 0, validate = {Validator.require})
    public OntologyActionRule sideEffectRule;

    @Override
    public RuleGroup getRuleGroup() {
        return RuleGroup.SIDE_EFFECT;
    }

    @Override
    public List<RuleType> getAllowedRuleTypes() {
        return List.of(
                RuleType.NOTIFICATION,
                RuleType.WEBHOOK,
                RuleType.WRITEBACK_WEBHOOK
        );
    }

    @Override
    public OntologyActionRule getRule() {
        return sideEffectRule;
    }

    @TISExtension
    public static class DefaultDescriptor extends BaseActionTypeDescriptor {

        @Override
        public RuleGroup getRuleGroup() {
            return RuleGroup.SIDE_EFFECT;
        }

        @Override
        public String getDisplayName() {
            return "Side Effect";
        }
    }
}