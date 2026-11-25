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
 * Webhook 调用失败的重试策略。
 *
 * <p>用于 {@link WebhookRule} 的 {@code retryOnFailure} 字段，是布尔开关的
 * 多态 Describable 化：原先的 {@code Boolean} 只能表达「重试 / 不重试」，
 * 撑不起重试策略本身的重试次数、延迟等配置面，现收敛为两个实现：
 * <ul>
 *   <li>{@link NoRetryPolicy} - 不重试（零字段 off 子类，默认）</li>
 *   <li>{@link AutoRetryPolicy} - 自动重试（承载固定重试策略常量）</li>
 * </ul>
 *
 * <p>与 {@link NotifyChannel}、{@link ValueSourceConfig} 同构：字段声明为基类类型
 * 不指定 {@code FormFieldType}，框架自动检测为多态嵌套子表单。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/11/23
 */
public abstract class RetryPolicy implements Describable<RetryPolicy>, PluginLiteria {

    public static abstract class BaseDescriptor extends Descriptor<RetryPolicy> {
    }
}
