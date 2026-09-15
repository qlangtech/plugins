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

/**
 * 发送通知规则
 *
 * Action 执行后发送通知给用户
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/6
 */
public class NotificationRule extends OntologyActionRule {

    @FormField(ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String recipientParam = "recipient";

    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String titleTemplate;

    @FormField(ordinal = 2, type = FormFieldType.TEXTAREA, validate = {Validator.require})
    public String messageTemplate;

    @FormField(ordinal = 3, type = FormFieldType.ENUM, validate = {})
    public String notificationType = "email";

    @FormField(ordinal = 4, type = FormFieldType.ENUM, validate = {})
    public String priority = "normal";

    @Override
    public RuleType getRuleType() {
        return RuleType.NOTIFICATION;
    }

    @Override
    public String identityValue() {
        return "notification";
    }

    @Override
    public RuleExecutionResult execute(RuleExecutionContext context) throws Exception {
        long startTime = System.currentTimeMillis();

        try {
            // 1. 获取收件人
            String recipient = context.getParameter(recipientParam, String.class);
            if (recipient == null) {
                return RuleExecutionResult.failure("未找到参数: " + recipientParam);
            }

            // 2. 解析模板，替换参数
            String title = replaceTemplate(titleTemplate, context);
            String message = replaceTemplate(messageTemplate, context);

            // 3. 发送通知（实际实现需要调用通知服务）
            // TODO: 集成通知服务
            // notificationService.sendNotification(
            //     notificationType,
            //     recipient,
            //     title,
            //     message,
            //     priority
            // );

            // 4. 将通知信息存入上下文
            context.setContextData("notificationSent", true);
            context.setContextData("notificationRecipient", recipient);

            long executionTime = System.currentTimeMillis() - startTime;

            return RuleExecutionResult.builder()
                .success(true)
                .putResultData("recipient", recipient)
                .putResultData("title", title)
                .putResultData("notificationType", notificationType)
                .putResultData("priority", priority)
                .executionTimeMs(executionTime)
                .build();

        } catch (Exception e) {
            return RuleExecutionResult.failure(e);
        }
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
            return RuleType.NOTIFICATION;
        }
    }
}
