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
package com.qlangtech.tis.plugin.ontology.workshop.widget;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;

/**
 * 自适应尺寸：由内容撑开，可选一个尺寸上限（防止超长 / 超宽内容撑爆画布）。
 * <p>
 * 对应前端 {@code {type: 'auto', maxSize?: number}}。
 *
 * <h3>为什么上限字段叫 {@code maxSize} 而不是 {@code maxHeight}</h3>
 * 本类同时服务于<b>宽度</b>与<b>高度</b>两个槽位（{@link WidgetDisplayConfig#width}
 * / {@link WidgetDisplayConfig#height}、{@link CompactDisplayConfig#width}），
 * 字段名若带轴向，用户在「宽度 → 自适应」下就会读到一个「最大高度」输入框 ——
 * 名字与父字段直接冲突，且单行控件根本没有内容区高度可言。{@code maxSize} 只说
 * 「自适应方向的尺寸上限」，两个槽位下都成立（宽度槽位即最大宽度）。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/13
 */
public class AutoSizingMode extends SizingMode {

    private static final long serialVersionUID = 1L;

    /**
     * 尺寸上限（像素），留空表示不限制：宽度槽位即最大宽度，高度槽位即最大高度
     */
    @FormField(ordinal = 0, type = FormFieldType.INT_NUMBER, validate = {Validator.require, Validator.integer})
    public Integer maxSize;

    @TISExtension
    public static class DefaultDescriptor extends BasicDescriptor implements DescriptorUseableShortComment {
        @Override
        public String getDisplayName() {
            return "Auto";
        }

        @Override
        public String shortComment() {
            return "自适应（内容撑开，可设最大高度）";
        }
    }
}
