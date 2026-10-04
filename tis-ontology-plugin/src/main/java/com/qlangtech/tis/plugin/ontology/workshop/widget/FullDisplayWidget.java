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

import com.qlangtech.tis.plugin.annotation.FormField;

/**
 * <b>有内容区</b>的 Widget 的基类：显示配置为完整四项
 * （宽度 / 高度 / 条件可见性 / 展示优化）。
 *
 * <p>它与 {@link CompactDisplayWidget} 的分工、判定口径、以及「中间抽象基类不得带
 * {@code @TISExtension}」这条硬约束，统一记在 {@link CompactDisplayWidget} 的类注释里，
 * 此处不再重复。简言之：本组的 Widget 内容长短不定（表格行数、图表、看板卡片、
 * Markdown 正文…），高度与挂载策略都说得通；本仓仅有的两个声明了「自然高度」的
 * Widget（{@code ObjectTableWidget} 的 {@code defaultSize.height=320}、
 * {@code ObjectListWidget} 的 400）都在本组，可作为分组是否合理的旁证。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/20
 * @see CompactDisplayWidget
 * @see WidgetDisplayConfig
 */
public abstract class FullDisplayWidget extends WorkshopWidget {

    private static final long serialVersionUID = 1L;

    /** 实例级显示配置（宽度 / 高度 / 条件可见性 / 展示优化）—— 字段名与 {@link CompactDisplayWidget} 一致，前端读取路径两组通用 */
    @FormField(ordinal = 4)
    public WidgetDisplayConfig displayConfig;
}
