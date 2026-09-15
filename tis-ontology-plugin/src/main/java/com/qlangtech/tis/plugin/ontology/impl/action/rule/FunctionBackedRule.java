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

import java.util.HashMap;
import java.util.Map;

/**
 * 函数支持规则
 *
 * 通过自定义函数执行复杂业务逻辑
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/6
 */
public class FunctionBackedRule extends OntologyActionRule {

    @FormField(ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String functionId;

    @FormField(ordinal = 1, type = FormFieldType.TEXTAREA, validate = {})
    public String parameterMappings;

    @FormField(ordinal = 2, type = FormFieldType.ENUM, validate = {})
    public Boolean async = false;

    @FormField(ordinal = 3, type = FormFieldType.INT_NUMBER, validate = {})
    public Integer timeoutSeconds = 60;

    @Override
    public RuleType getRuleType() {
        return RuleType.FUNCTION_BACKED;
    }

    @Override
    public String identityValue() {
        return "function_backed";
    }

    @Override
    public RuleExecutionResult execute(RuleExecutionContext context) throws Exception {
        long startTime = System.currentTimeMillis();

        try {
            // 1. 解析参数映射
            Map<String, String> mappings = parseParameterMappings(parameterMappings);

            // 2. 构建函数输入参数
            Map<String, Object> functionInputs = buildFunctionInputs(mappings, context.getParameters());

            // 3. 调用函数（实际实现需要调用 Function 服务）
            // TODO: 集成 Function 执行服务
            Object functionResult = null;
            // if (async) {
            //     // 异步执行
            //     functionResult = functionService.executeAsync(
            //         functionId,
            //         functionInputs,
            //         timeoutSeconds * 1000
            //     );
            // } else {
            //     // 同步执行
            //     functionResult = functionService.executeSync(
            //         functionId,
            //         functionInputs,
            //         timeoutSeconds * 1000
            //     );
            // }

            // 4. 将函数结果存入上下文
            context.setContextData("functionResult", functionResult);
            context.setContextData("functionId", functionId);

            long executionTime = System.currentTimeMillis() - startTime;

            return RuleExecutionResult.builder()
                .success(true)
                .putResultData("functionId", functionId)
                .putResultData("async", async)
                .putResultData("functionResult", functionResult)
                .executionTimeMs(executionTime)
                .build();

        } catch (Exception e) {
            return RuleExecutionResult.failure(e);
        }
    }

    /**
     * 解析参数映射配置
     * 格式: functionParam1=actionParam1\nfunctionParam2=actionParam2
     */
    private Map<String, String> parseParameterMappings(String mappings) {
        Map<String, String> result = new HashMap<>();
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
     * 根据映射构建函数输入参数
     */
    private Map<String, Object> buildFunctionInputs(Map<String, String> mappings,
                                                    Map<String, Object> actionParameters) {
        Map<String, Object> inputs = new HashMap<>();
        for (Map.Entry<String, String> entry : mappings.entrySet()) {
            String functionParam = entry.getKey();
            String actionParam = entry.getValue();
            Object paramValue = actionParameters.get(actionParam);
            if (paramValue != null) {
                inputs.put(functionParam, paramValue);
            }
        }
        return inputs;
    }

    @TISExtension
    public static class DefaultDescriptor extends BaseRuleDescriptor {

        @Override
        public RuleType getRuleType() {
            return RuleType.FUNCTION_BACKED;
        }
    }
}
