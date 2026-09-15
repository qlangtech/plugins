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

    @FormField(ordinal = 0, type = FormFieldType.TEXTAREA, validate = {Validator.require})
    public String propertyMappings;

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

            // 2. 解析属性映射配置
            Map<String, String> mappings = parsePropertyMappings(propertyMappings);

            // 3. 构建要更新的属性值
            Map<String, Object> updatedProperties = buildObjectProperties(mappings, context.getParameters());

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

    /**
     * 解析属性映射配置
     * 格式: propertyName1=paramName1\npropertyName2=paramName2
     */
    private Map<String, String> parsePropertyMappings(String mappings) {
        Map<String, String> result = new java.util.HashMap<>();
        if (mappings == null || mappings.trim().isEmpty()) {
            return result;
        }

        for (String line : mappings.split("\n")) {
            line = line.trim();
            if (line.isEmpty() || line.startsWith("#")) {
                continue;
            }
            String[] parts = line.split("=", 2);
            if (parts.length == 2) {
                result.put(parts[0].trim(), parts[1].trim());
            }
        }
        return result;
    }

    /**
     * 根据映射构建对象属性值
     */
    private Map<String, Object> buildObjectProperties(Map<String, String> mappings,
                                                     Map<String, Object> parameters) {
        Map<String, Object> properties = new java.util.HashMap<>();
        for (Map.Entry<String, String> entry : mappings.entrySet()) {
            String propertyName = entry.getKey();
            String paramName = entry.getValue();
            Object paramValue = parameters.get(paramName);
            if (paramValue != null) {
                properties.put(propertyName, paramValue);
            }
        }
        return properties;
    }

    @TISExtension
    public static class DefaultDescriptor extends BaseRuleDescriptor {

        @Override
        public RuleType getRuleType() {
            return RuleType.MODIFY_OBJECT;
        }
    }
}