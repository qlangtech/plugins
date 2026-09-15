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
import com.qlangtech.tis.plugin.ontology.impl.action.rule.FunctionBackedRule;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.OntologyActionRule;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.RuleGroup;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.RuleType;

import java.util.List;

/**
 * Function Action Type - 函数操作
 * <p>
 * 对应 Ontology Function 操作：
 * - FUNCTION_BACKED: 通过自定义函数执行复杂业务逻辑
 * <p>
 * 约束：FunctionActionType 不能与其他 Ontology rules（Object/Link/Interface）共存
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/7
 */
public class FunctionActionType extends ActionType {

    @FormField(ordinal = 0, validate = {Validator.require})
    public FunctionBackedRule functionRule;

    @Override
    public RuleGroup getRuleGroup() {
        return RuleGroup.ONTOLOGY_FUNCTION;
    }

    @Override
    public List<RuleType> getAllowedRuleTypes() {
        return List.of(RuleType.FUNCTION_BACKED);
    }

    @Override
    public OntologyActionRule getRule() {
        return functionRule;
    }

    @TISExtension
    public static class DefaultDescriptor extends BaseActionTypeDescriptor {

        @Override
        public RuleGroup getRuleGroup() {
            return RuleGroup.ONTOLOGY_FUNCTION;
        }

        @Override
        public String getDisplayName() {
            return "Function";
        }
    }
}