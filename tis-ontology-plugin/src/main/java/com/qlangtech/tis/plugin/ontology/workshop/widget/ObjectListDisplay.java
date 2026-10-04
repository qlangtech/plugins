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
 * ObjectList 的展示形态（Widget display）—— 列表 / 网格二选一。
 * <p>
 * 对应官方文档的 <b>Widget display</b> 设置：默认按列表显示，也可切换为网格，
 * 切换为网格后可再指定行数/列数、卡片尺寸、卡片间距。
 *
 * <h3>为什么是两个子类、而不是「一个 mode 枚举 + 若干可空载荷字段」</h3>
 * 后者允许出现 {@code mode=GRID} 却填了「属性对齐方式」这类<b>自相矛盾</b>的状态，
 * 编译器无从约束；而「固定取值集合 + 每种取值有各自的载荷」正是 TIS 中 Describable
 * 多态要解决的问题 —— 选了 Grid 就只渲染 Grid 的字段，选了 List 就只渲染 List 的字段。
 * 这与 {@link SizingMode}、{@code VariableDefinitionConfig}、{@code OverlayTypeConfig}
 * 的处理方式一致。
 * <p>
 * <b>不要</b>在此多态体系旁再挂一个同长度的 {@code WidgetDisplay} 枚举 —— 那是重复的
 * 分类体系（前车之鉴：{@code WorkshopWidget.type} / {@code WidgetType}）。
 *
 * <h3>默认项</h3>
 * 默认选中 {@link GridDisplay}，由父类 {@code ObjectListWidget.json} 里
 * {@code display} 字段的 {@code "dftVal": "Grid"} <b>逐字匹配</b>子类 descriptor 的
 * {@code getDisplayName()} 决定（前端 {@code tis.plugin.ts:1639-1650}），Java 侧无编译期保护。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/19
 */
public abstract class ObjectListDisplay implements Describable<ObjectListDisplay>, Serializable {

    private static final long serialVersionUID = 1L;

    public abstract static class BasicDescriptor extends Descriptor<ObjectListDisplay> {
    }
}