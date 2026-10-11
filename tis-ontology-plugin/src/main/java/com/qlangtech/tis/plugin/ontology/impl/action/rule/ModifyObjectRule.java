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

import java.util.List;
import java.util.Map;

/**
 * 修改对象规则
 *
 * 修改现有对象的属性值
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/6
 */
public class ModifyObjectRule extends OntologyActionRule {

    /**
     * 对象属性 ← 取值来源的映射行。
     *
     * @see OntologyActionRule#buildMappedValues(java.util.List, java.util.Map)
     */
    @FormField(ordinal = 0, type = FormFieldType.MULTI_DESCRIBLE_PLUGIN,
            desClazz = PropertyMappingConfig.class, validate = {Validator.require})
    public List<PropertyMappingConfig> propertyMappings;

    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
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
    public RuleExecutionResult execute(RuleExecutionContext context) throws Exception {
        long startTime = System.currentTimeMillis();

        try {
            // 1. 获取目标对象 RID
            String objectRid = context.getParameter(ridParameterName, String.class);
            if (objectRid == null) {
                return RuleExecutionResult.failure("未找到参数: " + ridParameterName);
            }

            // 2. 构建要更新的属性值
            Map<String, Object> updatedProperties = buildMappedValues(propertyMappings, context.getParameters());

            // 4. 修改对象（实际实现需要调用 Ontology 服务）
            // TODO: 集成 Ontology 对象修改服务
            // ontologyObjectService.modifyObject(context.getOntologyDomain(),
            //     context.getTargetObjectType(), objectRid, updatedProperties);

            // 5. 将修改的对象 RID 存入上下文
            context.setContextData("modifiedObjectRid", objectRid);

            long executionTime = System.currentTimeMillis() - startTime;

            return RuleExecutionResult.builder()
                .success(true)
                .addAffectedObjectRid(objectRid)
                .putResultData("objectRid", objectRid)
                .putResultData("objectType", context.getTargetObjectType())
                .putResultData("updatedProperties", updatedProperties.keySet())
                .executionTimeMs(executionTime)
                .build();

        } catch (Exception e) {
            return RuleExecutionResult.failure(e);
        }
    }

    @TISExtension
    public static class DefaultDescriptor extends BaseRuleDescriptor {

        @Override
        public RuleType getRuleType() {
            return RuleType.MODIFY_OBJECT;
        }
    }
}