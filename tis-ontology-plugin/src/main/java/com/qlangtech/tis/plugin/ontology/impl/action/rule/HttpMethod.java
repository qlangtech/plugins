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

/**
 * Webhook 请求的 HTTP 方法。
 *
 * <p>常量名即表单选项值 / 落盘值（与 WebhookRule.md 中 httpMethod 一节对应）；
 * 中文说明经 {@link #shortComment()} 作为选项 help 下发
 * （{@code OptionWithEndType.createEnumOptions}）。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/11/23
 */
public enum HttpMethod implements DescriptorUseableShortComment {

    POST("创建资源或提交数据"),
    PUT("更新资源"),
    PATCH("部分更新资源"),
    DELETE("删除资源"),
    GET("查询资源（不推荐，GET 请求通常不携带请求体）");

    private final String comment;

    HttpMethod(String comment) {
        this.comment = comment;
    }

    /**
     * 中文说明，作为表单选项的 help 下发（{@code createEnumOptions}）。
     */
    @Override
    public String shortComment() {
        return this.comment;
    }
}
