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

    /**
     * 对象属性 ← 取值来源的映射行。
     *
     * @see OntologyActionRule#buildMappedValues(java.util.List, java.util.Map)
     */
    @FormField(ordinal = 0, type = FormFieldType.MULTI_DESCRIBLE_PLUGIN,
            desClazz = PropertyMappingConfig.class, validate = {Validator.require})
    public List<PropertyMappingConfig> propertyMappings;

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
            // 1. 构建对象属性值
            Map<String, Object> objectProperties = buildMappedValues(propertyMappings, context.getParameters());

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