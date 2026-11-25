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

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.datax.transformer.UDFDesc;

import java.time.Duration;
import java.util.List;

/**
 * 自动重试（重试开关的 on 档）。
 *
 * <p>当前不暴露任何表单字段，重试策略按 WebhookRule.md 中的约定<b>固定</b>：
 * 失败后延迟重试一次；仅在网络错误或 5xx 状态码时触发，4xx（客户端错误）不重试。
 * 策略常量挂在{@link AutoRetryPolicy}上，供后续 HTTP 执行器消费；
 * 将来若策略需要可配置，在此类上加 {@code FormField} 字段即可，无需再动 {@link WebhookRule}。
 *
 * <p>注意事项（WebhookRule.md）：确保 Webhook 接口支持幂等性，避免重复触发外部副作用；
 * 重试延迟会叠加 {@code timeoutSeconds} 拉长总执行时长。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/11/23
 */
public class AutoRetryPolicy extends RetryPolicy {

    /**
     * 失败后额外尝试的次数（不含首次调用）。
     */
    public static final int MAX_RETRIES = 1;

    /**
     * 重试前的延迟时长。
     */
    public static final Duration RETRY_DELAY = Duration.ofSeconds(2);

    @Override
    public List<UDFDesc> getLiteria() {
        return List.of(new UDFDesc("policy", "自动重试") //
                , new UDFDesc("maxRetries", String.valueOf(MAX_RETRIES)));
    }

    @TISExtension
    public static class DftDescriptor extends BaseDescriptor implements DescriptorUseableShortComment {

        @Override
        public String getDisplayName() {
            return "自动重试";
        }

        @Override
        public String shortComment() {
            return "失败后延迟重试一次（仅网络错误或 5xx 重试，4xx 不重试），适用于网络抖动或临时故障场景";
        }
    }
}
