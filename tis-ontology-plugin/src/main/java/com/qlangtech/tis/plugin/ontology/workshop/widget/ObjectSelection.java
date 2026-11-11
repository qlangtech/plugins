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

import java.io.Serializable;

/**
 * Widget 选择策略 —— 只能单选（{@link NoObjectSelection}）/ 多选并把集合写回变量
 * （{@link DefaultObjectSelection}）二选一。
 *
 * <p><b>单选始终可用</b>：无论哪个子类，点击对象都会写回宿主的 {@code activeObjectVar}。
 * 本族描述的只是「是否额外支持多选」，开启态必须绑定一个写回变量 —— 否则勾了也白勾。
 *
 * <p>为什么是多态而不是原先的「{@code enableMultiSelect} + {@code selectedObjectsVar}」两个字段：
 * 那两者能拼出自相矛盾的状态（开了多选却没绑变量、绑了变量却没开开关），多态把「带哪些载荷」
 * 交给子类，与 {@link Pagenator}、{@link ListSorting} 同一处理方式。
 *
 * <p>关态子类的 {@code getDisplayName()} 必须是 {@link Descriptor#SWITCH_OFF}，
 * 且 {@code ObjectListWidget.json} 里 {@code objectSelection.dftVal} 要与它逐字相同，否则新建的
 * Widget 会静默落到开启态上（不报错）。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/11/10
 */
public abstract class ObjectSelection implements Describable<ObjectSelection>, Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 是否支持多选。false 时前端不渲染勾选框。
     */
    public abstract boolean multiSelectEnabled();

    /**
     * 选中集合写回的对象集变量名。关态返回 {@code null}。
     */
    public abstract String resolveSelectedObjectsVar();

    /**
     * 注意：本基类与其 {@code BasicDescriptor} <b>都不得</b>带 {@code @TISExtension}，
     * 也不得有嵌套的 Descriptor —— sezpoz 索引不过滤抽象类，加了会让抽象类型静默混进插件面板。
     * 子类的 Descriptor 用无参构造（{@code clazz = getClass().getEnclosingClass()}），
     * 故必须嵌在<b>具体子类</b>里，否则两个子类的 {@code getId()} 会撞成同一个。
     */
    public abstract static class BasicDescriptor extends Descriptor<ObjectSelection> {
    }
}
