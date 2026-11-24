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
import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;

/**
 * 「不配置」形态的 {@link DisplayConfig}：前端沿用框架缺省 —— 紧凑垂直布局 + 600px 宽
 * （即 displayConfig 引入前 action-form-modal 的硬编码行为，向后兼容）。
 *
 * <p>零字段是刻意的 —— 既然不配置就没有可配项，因此本类<b>没有</b>对应的 {@code .json}
 * （没有 {@code @FormField} 就无处可写）。
 *
 * <p>{@link #getDisplayName()} 返回 {@link Descriptor#SWITCH_OFF}（即 {@code "off"}）
 * 必须与 {@code ActionParameters.json} 里 {@code displayConfig} 字段的 {@code dftVal}
 * 逐字相同，否则新建表单的默认选中项会静默漂移。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/11/21
 * @see DisplayConfigOn
 */
public class DisplayConfigOff extends DisplayConfig {

    @TISExtension
    public static class DefaultDescriptor extends Descriptor<DisplayConfigOff>
            implements DescriptorUseableShortComment {

        @Override
        public String getDisplayName() {
            return SWITCH_OFF;
        }

        @Override
        public String shortComment() {
            return "使用框架缺省布局与宽度";
        }
    }
}
