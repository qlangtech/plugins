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

import java.util.List;

/**
 * 不重试（重试开关的 off 档）。
 *
 * <p>零字段实现：请求失败后立即返回错误。适用于幂等性要求高、
 * 或外部副作用不可重复触发的 Webhook 场景。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/11/23
 */
public class NoRetryPolicy extends RetryPolicy {

    /**
     * 零字段实现，literia 只有策略语义标签：渠道/策略的「类型」信息不在数据里，
     * 不写的话 tuples 视图里 {@code retryOnFailure: []} 键后头空空如也。
     */
    @Override
    public List<UDFDesc> getLiteria() {
        return List.of(new UDFDesc("policy", "不重试"));
    }

    @TISExtension
    public static class DftDescriptor extends BaseDescriptor implements DescriptorUseableShortComment {

        @Override
        public String getDisplayName() {
            return "不重试";
        }

        @Override
        public String shortComment() {
            return "请求失败后立即返回错误，适用于幂等性要求高的场景";
        }
    }
}
