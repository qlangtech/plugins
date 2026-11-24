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

package com.qlangtech.tis.plugin.ontology.impl.action.rule;

import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;

import java.util.Collections;

import java.util.List;

/**
 * 修改对象规则
 * <p>
 * 修改现有对象的属性值
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/6
 */
public class ModifyObjectRule extends OntologyActionRule {

    public static final String KEY_RID_PARAMETER_NAME = "ridParameterName";

    /**
     * 对象属性 ← 取值来源的映射行。
     *
     * @see OntologyActionRule#buildMappedValues(java.util.List, java.util.Map)
     */
    @FormField(ordinal = 0, type = FormFieldType.MULTI_DESCRIBLE_PLUGIN,
            desClazz = PropertyMappingConfig.class, validate = {Validator.require})
    public List<PropertyMappingConfig> propertyMappings;

    @FormField(ordinal = 1, type = FormFieldType.SELECTABLE, validate = {Validator.require})
    public String ridParameterName = "rid";

    @Override
    public RuleType getRuleType() {
        return RuleType.MODIFY_OBJECT;
    }

    @Override
    public String identityValue() {
        return "modify_object";
    }


    @Override
    public void accept(AdapterRuleVisitor visitor) {
        visitor.visit(this);
    }

    @TISExtension
    public static class DefaultDescriptor extends BaseRuleDescriptor {

        public DefaultDescriptor() {
            super();
            // SELECTABLE 必须在 descriptor 上登记候选来源，否则 DescriptorsJSON#getSelectOptions 直接抛异常、
            // 整张表单打不开。登记空列表作兜底：真实候选由宿主 ActionType 的级联管道在渲染期回填。
            this.registerSelectOptions("ridParameterName", Collections::emptyList);
        }

        @Override
        public RuleType getRuleType() {
            return RuleType.MODIFY_OBJECT;
        }
    }
}