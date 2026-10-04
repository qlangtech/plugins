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

/**
 * 列表形态：每个对象占一行，属性在其后铺开。
 * <p>
 * 官方文档里 List 是默认形态，但在本工程中默认给了 {@link GridDisplay}
 * —— 前端 {@code object-list.component.ts} 目前只会渲网格，默认 Grid 才不会造成视觉回退。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/19
 */
public class ListDisplay extends ObjectListDisplay {

    private static final long serialVersionUID = 1L;

    /** 属性与值的排布方式 */
    @FormField(ordinal = 0, type = FormFieldType.ENUM)
    public PropertyStyle propertyStyle = PropertyStyle.inline;

    /**
     * 属性排布方式；取值固定，硬编码于此（CLAUDE.md 原则二：固定取值用 ENUM）。
     * <p>
     * 中文 label 写在 {@code ListDisplay.json} 的 {@code enum} 数组里 —— ENUM 的下拉 label
     * 取的是<b>常量名</b>，不写 json 数组用户看到的就是 {@code inline} / {@code aligned}。
     */
    public enum PropertyStyle implements DescriptorUseableShortComment {
        /** 名称与值同行，如「现价 • 12.34」 */
        inline("内联（名称与值同行）"),
        /** 名称与值分列对齐，如「现价    12.34」 */
        aligned("对齐（名称与值分列）");

        private final String comment;

        PropertyStyle(String comment) {
            this.comment = comment;
        }

        @Override
        public String shortComment() {
            return comment;
        }
    }

    @TISExtension
    public static class DefaultDescriptor extends BasicDescriptor implements DescriptorUseableShortComment {
        @Override
        public String getDisplayName() {
            return "List";
        }

        @Override
        public String shortComment() {
            return "列表：每个对象一行，属性在其后铺开";
        }
    }
}