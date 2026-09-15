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
import java.util.UUID;

/**
 * 创建对象规则
 *
 * 根据参数值创建新的对象实例
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/6
 */
public class CreateObjectRule extends OntologyActionRule {

    @FormField(ordinal = 0, type = FormFieldType.TEXTAREA, validate = {Validator.require})
    public String propertyMappings;

    @FormField(ordinal = 1, type = FormFieldType.ENUM, validate = {})
    public Boolean generateRid = true;

    @Override
    public RuleType getRuleType() {
        return RuleType.CREATE_OBJECT;
    }

    @Override
    public String identityValue() {
        return "create_object";
    }

    @Override
    public RuleExecutionResult execute(RuleExecutionContext context) throws Exception {
        long startTime = System.currentTimeMillis();

        try {
            // 1. 解析属性映射配置
            Map<String, String> mappings = parsePropertyMappings(propertyMappings);

            // 2. 构建对象属性值
            Map<String, Object> objectProperties = buildObjectProperties(mappings, context.getParameters());

            // 3. 生成或获取对象 RID
            String objectRid = generateRid ? generateObjectRid() : context.getParameter("rid", String.class);
            if (objectRid == null) {
                return RuleExecutionResult.failure("未指定对象 RID 且未启用自动生成");
            }

            // 4. 创建对象（实际实现需要调用 Ontology 服务）
            // TODO: 集成 Ontology 对象创建服务
            // ontologyObjectService.createObject(context.getOntologyDomain(),
            //     context.getTargetObjectType(), objectRid, objectProperties);

            // 5. 将创建的对象 RID 存入上下文，供后续规则使用
            context.setContextData("createdObjectRid", objectRid);

            long executionTime = System.currentTimeMillis() - startTime;

            return RuleExecutionResult.builder()
                .success(true)
                .addAffectedObjectRid(objectRid)
                .putResultData("objectRid", objectRid)
                .putResultData("objectType", context.getTargetObjectType())
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

    /**
     * 生成对象 RID
     */
    private String generateObjectRid() {
        return UUID.randomUUID().toString().replace("-", "");
    }

    @TISExtension
    public static class DefaultDescriptor extends BaseRuleDescriptor {

        @Override
        public RuleType getRuleType() {
            return RuleType.CREATE_OBJECT;
        }


    }
}