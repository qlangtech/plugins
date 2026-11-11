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

import java.util.Collections;

/**
 * 按指定属性排序，并在列表上方展示排序字段 / 方向选择器（用户可改）。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/11/10
 */
public class DefaultListSorting extends ListSorting {

    private static final long serialVersionUID = 1L;

    public static final String KEY_SORT_FIELD = "sortField";
    public static final String KEY_SORT_ORDER = "sortOrder";

    /**
     * 排序属性。
     *
     * <p>必须是 {@code SELECTABLE} 而不是 {@code INPUTTEXT}：候选由宿主 Widget 的
     * {@code objectSetVar} 级联管道供给（管道目标是 {@code listSorting.sortField}），改成自由文本
     * 就等于让级联结果没有地方落地。也不该是 {@code ENUM}：候选是运行时数据（本体属性清单），
     * 不是固定枚举。
     */
    @FormField(ordinal = 0, type = FormFieldType.SELECTABLE, validate = {Validator.require})
    public String sortField;

    /**
     * 排序方向。
     *
     * <p>中文取值由 {@link SortOrder#shortComment()} 提供，{@code .json} 里只写 {@code dftVal}。
     */
    @FormField(ordinal = 1, type = FormFieldType.ENUM, validate = {Validator.require})
    public SortOrder sortOrder = SortOrder.asc;

    @Override
    public boolean sortingEnabled() {
        return true;
    }

    @Override
    public String resolveSortField() {
        return sortField;
    }

    @Override
    public SortOrder resolveSortOrder() {
        // 未填时回落 asc：与 json 的 dftVal 及前端归一化器的兜底值三处保持一致
        return sortOrder != null ? sortOrder : SortOrder.asc;
    }

    /**
     * 无参构造是必需的：{@code clazz = getClass().getEnclosingClass()} 靠它把 descriptor 绑到
     * {@code DefaultListSorting} 上，写成带参（或移到基类里）两个子类的 {@code getId()} 会撞号。
     */
    @TISExtension
    public static class DefaultDescriptor extends BasicDescriptor implements DescriptorUseableShortComment {

        public DefaultDescriptor() {
            super();
            // 兜底注册：真实候选由 ObjectListWidget 的级联管道在渲染期回填。
            // SELECTABLE 字段未注册会让 DescriptorsJSON 抛 "has not been register"，整张表单打不开。
            this.registerSelectOptions(KEY_SORT_FIELD, Collections::emptyList);
        }

        @Override
        public String getDisplayName() {
            return "Default";
        }

        @Override
        public String shortComment() {
            return "按指定属性排序，用户可在列表上方切换";
        }
    }
}
