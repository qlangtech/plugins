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

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.DescriptorUseableShortComment;

import java.io.Serializable;

/**
 * Widget 列表排序策略 —— 不排序（{@link NoListSorting}）/ 按指定属性排序（{@link DefaultListSorting}）二选一。
 *
 * <p>为什么是多态而不是原先的「{@code defaultSortField} + {@code defaultSortOrder} + {@code enableUserSorting}」
 * 三个字段：那三个字段能拼出自相矛盾的状态 —— 关掉了 {@code enableUserSorting} 却还填着默认排序字段，
 * 「到底排不排」「用户能不能改」读配置的人无从判断。多态把「带哪些载荷」交给子类，与
 * {@link Pagenator}、{@link SizingMode} 同一处理方式。
 *
 * <p><b>开启态即「可改」</b>：{@code Default} 表示既有默认排序，又在列表上方渲染排序字段 / 方向选择器。
 * 原先「固定默认排序但用户不可改」这一组合被有意去掉（要固定就干脆不排，两者价值都不大却要多一个开关）。
 *
 * <p>关态子类的 {@code getDisplayName()} 必须是 {@link Descriptor#SWITCH_OFF}，
 * 且 {@code ObjectListWidget.json} 里 {@code listSorting.dftVal} 要与它逐字相同，否则新建的
 * Widget 会静默落到开启态上（不报错）。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/11/10
 */
public abstract class ListSorting implements Describable<ListSorting>, Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 是否排序。false 时列表保持变量返回的原始顺序。
     */
    public abstract boolean sortingEnabled();

    /**
     * 生效的排序属性名。关态返回 {@code null}。
     */
    public abstract String resolveSortField();

    /**
     * 生效的排序方向。关态返回 {@code null}。
     */
    public abstract SortOrder resolveSortOrder();

    /**
     * 排序方向。
     *
     * <p>中文取值走 {@link #shortComment()}（ENUM 下拉的选项说明），{@code .json} 里<b>不写</b>
     * {@code enum} 数组 —— 手写会连带绕过 {@code endType} / {@code help} 的自动派生
     * （见 CLAUDE.md 原则二）。
     */
    public enum SortOrder implements DescriptorUseableShortComment {
        asc("升序"),
        desc("降序");

        private final String comment;

        SortOrder(String comment) {
            this.comment = comment;
        }

        @Override
        public String shortComment() {
            return comment;
        }
    }

    /**
     * 注意：本基类与其 {@code BasicDescriptor} <b>都不得</b>带 {@code @TISExtension}，
     * 也不得有嵌套的 Descriptor —— sezpoz 索引不过滤抽象类，加了会让抽象类型静默混进插件面板。
     * 子类的 Descriptor 用无参构造（{@code clazz = getClass().getEnclosingClass()}），
     * 故必须嵌在<b>具体子类</b>里，否则两个子类的 {@code getId()} 会撞成同一个。
     */
    public abstract static class BasicDescriptor extends Descriptor<ListSorting> {
    }
}
