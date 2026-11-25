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

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.plugin.datax.transformer.PluginLiteria;

/**
 * 通知渠道抽象基类。
 *
 * <p>用于 {@link NotificationRule} 的 {@code notifyChannel} 字段，取代原先
 * 「{@code notificationType} 枚举 + {@code recipientParam} 参数名」的耦合设计：
 * 渠道类型由具体子类表达，收件人地址由子类自带字段承载，格式校验随渠道而定。
 *
 * <p>四个具体子类（与 NotificationRule.md 中 notificationType 一节对应）：
 * <ul>
 *   <li>{@link EmailNotifyChannel} - 邮件通知，收件人为邮箱地址</li>
 *   <li>{@link SmsNotifyChannel} - 短信通知，收件人为手机号码</li>
 *   <li>{@link WebhookNotifyChannel} - Webhook 回调，地址为 URL</li>
 *   <li>{@link ImNotifyChannel} - 即时消息（如钉钉、企业微信），目标为用户/群组 ID</li>
 * </ul>
 *
 * <p>与 {@link ValueSourceConfig} 同构：字段声明为基类类型不指定
 * {@code FormFieldType}，框架自动检测为多态嵌套子表单。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/11/23
 */
public abstract class NotifyChannel implements Describable<NotifyChannel>, PluginLiteria {

    public static abstract class BaseDescriptor extends Descriptor<NotifyChannel> {
    }
}
