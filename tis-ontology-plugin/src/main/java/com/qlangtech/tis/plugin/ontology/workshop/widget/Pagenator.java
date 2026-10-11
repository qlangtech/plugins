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
 * Widget 分页策略 —— 不开启分页（{@link NoPagenator}）/ 服务端分页（{@link DefaultPagenator}）二选一。
 *
 * <p>为什么是多态而不是「一个 {@code pageSize} 字段 + 一个 enable 布尔」：后者允许出现
 * 「关掉了分页却还填着每页条数」这类自相矛盾的配置，且「关闭分页」时那个 pageSize 到底还要不要
 * 参与取数，读配置的人无从判断。多态把「带哪些载荷」交给子类，与 {@link SizingMode}、
 * {@code VariableDefinitionConfig} 同一处理方式。
 *
 * <p>「关闭分页」并不等于「不取数」：关态下仍然取第一页（{@link #DEFAULT_PAGE_SIZE} 条），
 * 只是前端不渲染分页条 —— 表格总得有几行给人看。
 *
 * <p>关态子类的 {@code getDisplayName()} 必须是 {@link Descriptor#SWITCH_OFF}，
 * 且 {@code ObjectTableWidget.json} 里 {@code pagenator.dftVal} 要与它逐字相同，否则新建的
 * Widget 会静默落到另一个子类上（不报错）。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/9
 */
public abstract class Pagenator implements Describable<Pagenator>, Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 关闭分页（off）时取第一页用的条数。与前端 {@code DEFAULT_OBJECT_TABLE_PAGE_SIZE} 同值 ——
     * 两边不一致的话，关态下表里显示的条数会与后端实际取的条数对不上。
     */
    public static final int DEFAULT_PAGE_SIZE = 20;

    /**
     * 是否启用分页。false 时调用方只取第 1 页，且<b>不必</b>去查总条数
     * （前端没有分页条，总数也无处可显示）。
     */
    public abstract boolean pagingEnabled();

    /**
     * 本次请求生效的每页条数。未填时回落到 {@link #DEFAULT_PAGE_SIZE}。
     */
    public abstract int resolvePageSize();

    /**
     * 注意：本基类与其 {@code BasicDescriptor} <b>都不得</b>带 {@code @TISExtension}，
     * 也不得有嵌套的 Descriptor —— sezpoz 索引不过滤抽象类，加了会让抽象类型静默混进插件面板。
     * 子类的 Descriptor 用无参构造（{@code clazz = getClass().getEnclosingClass()}），
     * 故必须嵌在<b>具体子类</b>里，否则两个子类的 {@code getId()} 会撞成同一个。
     */
    public abstract static class BasicDescriptor extends Descriptor<Pagenator> {
    }
}
