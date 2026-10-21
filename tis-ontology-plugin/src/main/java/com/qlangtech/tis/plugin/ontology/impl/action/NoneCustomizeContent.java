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

package com.qlangtech.tis.plugin.ontology.impl.action;

import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;

import java.util.Optional;

/**
 * 「不定制」形态的 {@link CustomizeContent}：照用框架缺省的提交按钮文案 / 成功提示。
 *
 * <p>零字段是刻意的 —— 既然不定制就没有可配项，因此本类<b>没有</b>对应的 {@code .json}
 * （没有 {@code @FormField} 就无处可写）。
 *
 * <p>{@link #getDisplayName()} 返回 {@link Descriptor#SWITCH_OFF}（即 {@code "off"}）
 * 是这套开关机制的<b>全部</b>：前端拿 {@code ObjectActionType.json} 里该字段的
 * {@code dftVal} 逐项比对各子描述符的 {@code displayName} 来选默认项，两侧字符串必须
 * 逐字相同，改成别的字符串默认项就悄悄丢了。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/14
 * @see DefaultCustomizeContent
 */
public class NoneCustomizeContent extends CustomizeContent {

    @Override
    public Optional<String> content() {
        return Optional.empty();
    }

    @TISExtension
    public static class DefaultDescriptor extends Descriptor<NoneCustomizeContent> {

        @Override
        public String getDisplayName() {
            return SWITCH_OFF;
        }
    }
}
