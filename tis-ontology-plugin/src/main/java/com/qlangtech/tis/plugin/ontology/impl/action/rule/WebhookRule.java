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

    /**
     * 请求头键值行；值里可以写 {@code ${paramName}}。
     */
    @FormField(ordinal = 2, type = FormFieldType.MULTI_DESCRIBLE_PLUGIN,
            desClazz = HttpHeaderConfig.class, validate = {})
    public List<HttpHeaderConfig> headers;

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

//    @Override
//    public void execute(Describable<?> plugin) {
//        Map<String, Object> parameters = getParameterValues(plugin);
//
//        // 1. 请求头（同名后者覆盖前者）
//        Map<String, String> headerMap = headers == null ? Map.of()
//                : headers.stream().collect(Collectors.toMap(
//                (h) -> h.name, (h) -> h.value == null ? "" : h.value, (a, b) -> b));
//
//        // 2. 解析请求体模板
//        String requestBody = replaceTemplate(bodyTemplate, parameters);
//
//        // 3. 调用 Webhook（实际实现需要调用 HTTP 客户端）
//        // TODO: 集成 HTTP 客户端
//        // HttpResponse response = httpClient.request(webhookUrl, httpMethod, headerMap, requestBody,
//        //     timeoutSeconds * 1000);
//        //
//        // if (!response.isSuccess() && Boolean.TRUE.equals(retryOnFailure)) {
//        //     // 重试逻辑
//        //     response = httpClient.request(...);
//        // }
//    }

//    /**
//     * 替换模板中的参数占位符
//     * 格式: ${paramName}
//     */
//    private String replaceTemplate(String template, Map<String, Object> parameters) {
//        if (template == null) {
//            return "";
//        }
//
//        String result = template;
//        for (Map.Entry<String, Object> entry : parameters.entrySet()) {
//            if (entry.getValue() != null) {
//                result = result.replace("${" + entry.getKey() + "}", entry.getValue().toString());
//            }
//        }
//        return result;
//    }

    @Override
    public void accept(AdapterRuleVisitor visitor) {
        visitor.visit(this);
    }

    @TISExtension
    public static class DefaultDescriptor extends BaseRuleDescriptor {

        @Override
        public RuleType getRuleType() {
            return RuleType.WEBHOOK;
        }
    }
}
