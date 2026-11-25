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

import java.util.List;
import java.util.Map;

/**
 * 发送通知规则
 *
 * Action 执行后发送通知给用户
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/6
 */
public class NotificationRule extends OntologyActionRule {

    /**
     * 通知渠道配置。
     *
     * <p>多态字段，TIS 自动检测为嵌套子表单，前端渲染为渠道选择 + 对应表单。
     * 渠道类型与收件人地址强相关，所以收件人字段收进 {@link NotifyChannel} 各子类，
     * 由子类自带格式校验（邮箱/手机号/URL）。
     *
     * <p>四个可选实现：
     * <ul>
     *   <li>{@link EmailNotifyChannel} - 邮件通知</li>
     *   <li>{@link SmsNotifyChannel} - 短信通知</li>
     *   <li>{@link WebhookNotifyChannel} - Webhook 回调</li>
     *   <li>{@link ImNotifyChannel} - 即时消息</li>
     * </ul>
     */
    @FormField(ordinal = 0, validate = {Validator.require})
    public NotifyChannel notifyChannel;

    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String titleTemplate;

    @FormField(ordinal = 2, type = FormFieldType.TEXTAREA, validate = {Validator.require})
    public String messageTemplate;

    @FormField(ordinal = 3, type = FormFieldType.ENUM, validate = {})
    public NotifyPriority priority = NotifyPriority.normal;

    @Override
    public RuleType getRuleType() {
        return RuleType.NOTIFICATION;
    }

    @Override
    public String identityValue() {
        return "notification";
    }

    @Override
    public List<UDFDesc> getLiteria() {
        List<UDFDesc> literia = initLiteria();
        // 渠道是多态字段，半配置（未选渠道）时为 null，跳过该对不让整个 literia 抛
        if (this.notifyChannel != null) {
            literia.add(new UDFDesc("notifyChannel", this.notifyChannel.getLiteria()));
        }
        literia.add(new UDFDesc("titleTemplate", this.titleTemplate));
        literia.add(new UDFDesc("messageTemplate", this.messageTemplate));
        literia.add(new UDFDesc("priority", this.priority == null ? null : this.priority.name()));
        return literia;
    }

//    @Override
//    public void execute(Describable<?> plugin) {
//        Map<String, Object> parameters = getParameterValues(plugin);
//
//        // 1. 解析模板，替换参数
//        String title = replaceTemplate(titleTemplate, parameters);
//        String message = replaceTemplate(messageTemplate, parameters);
//
//        // 2. 发送通知（实际实现需要调用通知服务）
//        // TODO: 集成通知服务
//        // notifyChannel.send(title, message, priority);
//    }

    @Override
    public void accept(AdapterRuleVisitor visitor) {
        visitor.visit(this);
    }

    @TISExtension
    public static class DefaultDescriptor extends BaseRuleDescriptor {

        @Override
        public RuleType getRuleType() {
            return RuleType.NOTIFICATION;
        }
    }
}
