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
import com.qlangtech.tis.plugin.datax.transformer.UDFDesc;

import java.util.Collections;

import java.util.List;

/**
 * 删除对象规则
 * <p>
 * 删除指定的对象实例
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/6
 */
public class DeleteObjectRule extends OntologyActionRule {

    @FormField(ordinal = 0, type = FormFieldType.SELECTABLE, validate = {Validator.require})
    public String ridParameterName = "rid";

    // @FormField(ordinal = 1, type = FormFieldType.ENUM, validate = {})
    public Boolean cascadeDelete = false;

    @FormField(ordinal = 2, type = FormFieldType.ENUM, validate = {})
    public Boolean confirmationRequired = true;

    @Override
    public RuleType getRuleType() {
        return RuleType.DELETE_OBJECT;
    }

    @Override
    public String identityValue() {
        return "delete_object";
    }

    @Override
    public List<UDFDesc> getLiteria() {
        List<UDFDesc> literia = initLiteria();
        literia.add(new UDFDesc(ModifyObjectRule.KEY_RID_PARAMETER_NAME, this.ridParameterName));
        literia.add(new UDFDesc("cascadeDelete", String.valueOf(this.cascadeDelete)));
        literia.add(new UDFDesc("confirmationRequired", String.valueOf(this.confirmationRequired)));
        return literia;
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
            this.registerSelectOptions(ModifyObjectRule.KEY_RID_PARAMETER_NAME, Collections::emptyList);
        }

        @Override
        public RuleType getRuleType() {
            return RuleType.DELETE_OBJECT;
        }
    }
}
