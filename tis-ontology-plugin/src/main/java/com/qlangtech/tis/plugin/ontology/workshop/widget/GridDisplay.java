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
 * 网格形态：每个对象渲染成一张卡片，按列数排布。
 * <p>
 * 这是 ObjectList 的<b>默认</b>形态（见 {@link ObjectListDisplay} 的类注释）。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/19
 */
public class GridDisplay extends ObjectListDisplay {

    private static final long serialVersionUID = 1L;

    /** 每行展示的卡片数量（响应式布局下的最大列数） */
    @FormField(ordinal = 0, type = FormFieldType.INT_NUMBER, validate = {Validator.require})
    public Integer columns = 4;

    @TISExtension
    public static class DefaultDescriptor extends BasicDescriptor implements DescriptorUseableShortComment {
        @Override
        public String getDisplayName() {
            return "Grid";
        }

        @Override
        public String shortComment() {
            return "卡片网格：可设置每行卡片数量";
        }
    }
}