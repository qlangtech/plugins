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
 * Webhook 规则
 *
 * Action 执行后调用外部 Webhook API
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/6
 */
public class WebhookRule extends OntologyActionRule {

    @FormField(ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.url})
    public String webhookUrl;

    @FormField(ordinal = 1, type = FormFieldType.ENUM, validate = {Validator.require})
    public String httpMethod = "POST";

    @FormField(ordinal = 2, type = FormFieldType.TEXTAREA, validate = {})
    public String headers;

    @FormField(ordinal = 3, type = FormFieldType.TEXTAREA, validate = {})
    public String bodyTemplate;

    @FormField(ordinal = 4, type = FormFieldType.INT_NUMBER, validate = {})
    public Integer timeoutSeconds = 30;

    @FormField(ordinal = 5, type = FormFieldType.ENUM, validate = {})
    public Boolean retryOnFailure = false;

    @Override
    public RuleType getRuleType() {
        return RuleType.WEBHOOK;
    }

    @Override
    public String identityValue() {
        return "webhook";
    }

    @Override
    public RuleExecutionResult execute(RuleExecutionContext context) throws Exception {
        long startTime = System.currentTimeMillis();

        try {
            // 1. 解析请求头
            Map<String, String> headerMap = parseHeaders(headers);

            // 2. 解析请求体模板
            String requestBody = replaceTemplate(bodyTemplate, context);

            // 3. 调用 Webhook（实际实现需要调用 HTTP 客户端）
            // TODO: 集成 HTTP 客户端
            // HttpResponse response = httpClient.request(
            //     webhookUrl,
            //     httpMethod,
            //     headerMap,
            //     requestBody,
            //     timeoutSeconds * 1000
            // );
            //
            // if (!response.isSuccess() && retryOnFailure) {
            //     // 重试逻辑
            //     response = httpClient.request(...);
            // }

            // 4. 将调用结果存入上下文
            context.setContextData("webhookCalled", true);
            context.setContextData("webhookUrl", webhookUrl);

            long executionTime = System.currentTimeMillis() - startTime;

            return RuleExecutionResult.builder()
                .success(true)
                .putResultData("webhookUrl", webhookUrl)
                .putResultData("httpMethod", httpMethod)
                .putResultData("responseStatus", 200) // TODO: 实际响应状态
                .executionTimeMs(executionTime)
                .build();

        } catch (Exception e) {
            return RuleExecutionResult.failure(e);
        }
    }

    /**
     * 解析请求头配置
     * 格式: Header-Name: Header-Value\nAnother-Header: Another-Value
     */
    private Map<String, String> parseHeaders(String headersStr) {
        Map<String, String> result = new HashMap<>();
        if (headersStr == null || headersStr.trim().isEmpty()) {
            return result;
        }

        for (String line : headersStr.split("\n")) {
            line = line.trim();
            if (line.isEmpty() || line.startsWith("#")) {
                continue;
            }
            String[] parts = line.split(":", 2);
            if (parts.length == 2) {
                result.put(parts[0].trim(), parts[1].trim());
            }
        }
        return result;
    }

    /**
     * 替换模板中的参数占位符
     * 格式: ${paramName}
     */
    private String replaceTemplate(String template, RuleExecutionContext context) {
        if (template == null) {
            return "";
        }

        String result = template;
        for (String paramName : context.getParameters().keySet()) {
            Object paramValue = context.getParameter(paramName);
            if (paramValue != null) {
                result = result.replace("${" + paramName + "}", paramValue.toString());
            }
        }
        return result;
    }

    @TISExtension
    public static class DefaultDescriptor extends BaseRuleDescriptor {

        @Override
        public RuleType getRuleType() {
            return RuleType.WEBHOOK;
        }
    }
}
