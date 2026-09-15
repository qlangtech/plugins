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
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.OntologyActionRule;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.RuleGroup;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.RuleType;

import java.util.List;

/**
 * Object Action Type - 对象操作
 * <p>
 * 对应 Ontology Object 操作，包括：
 * - CREATE_OBJECT: 创建新对象
 * - MODIFY_OBJECT: 修改现有对象
 * - DELETE_OBJECT: 删除对象
 * - CREATE_OR_MODIFY_OBJECT: 创建或修改对象
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/7
 */
public class ObjectActionType extends ActionType {

    @FormField(ordinal = 0, type = FormFieldType.ENUM, validate = {Validator.require})
    public String objectTypeReference;

    @FormField(ordinal = 1, validate = {Validator.require})
    public OntologyActionRule objectRule;

    @Override
    public RuleGroup getRuleGroup() {
        return RuleGroup.ONTOLOGY_OBJECT;
    }

    @Override
    public List<RuleType> getAllowedRuleTypes() {
        return List.of(
                RuleType.CREATE_OBJECT,
                RuleType.MODIFY_OBJECT,
                RuleType.DELETE_OBJECT,
                RuleType.CREATE_OR_MODIFY_OBJECT
        );
    }

    @Override
    public OntologyActionRule getRule() {
        return objectRule;
    }

    @TISExtension
    public static class DefaultDescriptor extends BaseActionTypeDescriptor {

        @Override
        public RuleGroup getRuleGroup() {
            return RuleGroup.ONTOLOGY_OBJECT;
        }

        @Override
        public String getDisplayName() {
            return "Object";
        }
    }
}