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
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.ontology.workshop.model.ConditionalVisibility;

import java.io.Serializable;

/**
 * 单行表单控件的显示配置：只含<b>宽度</b>与<b>条件可见性</b>。
 *
 * <h3>与 {@link WidgetDisplayConfig} 的关系</h3>
 * 本类是 {@link WidgetDisplayConfig} 的<b>同名字段子集</b>：两个类的
 * {@code width} / {@code conditionalVisibility} 字段名、类型、ordinal 逐一相同
 * （{@code 0} / {@code 2} 与 {@code WidgetDisplayConfig} 对齐，便于读者对照），
 * 因此前端读 {@code widget.displayConfig.width} 对两组 Widget 走同一条路径。
 *
 * <h3>为什么不含 height / displayOptimization</h3>
 * 这两项对单行输入控件没有语义：
 * <ul>
 *   <li>{@code height} —— 控件高度由控件自身决定（一个日期选择器/复选框没有「内容区高度」
 *       的概念），给固定像素或 flex 比例都是空话。</li>
 *   <li>{@code displayOptimization} —— 它解决的是「重渲染代价大的 Widget（表格、图表）
 *       在进出视口时该不该卸载」的问题，而表单控件无所谓挂载开销，也不存在需要保留的
 *       内部滚动位置。</li>
 * </ul>
 * 让这些字段出现在表单里，用户会填了却发现没有任何效果，这正是把
 * {@link CompactDisplayWidget} / {@link FullDisplayWidget} 拆开的理由。
 *
 * <p>同理，宽度槽位在自适应下也<b>不该</b>冒出高度概念：{@link AutoSizingMode} 的上限字段
 * 叫 {@code maxSize} 而非 {@code maxHeight}，正是为了让「宽度 → 自适应」下的那个输入框
 * 不对着一行的宽度谈高度。
 *
 * <h3>谁在用</h3>
 * 由 {@link CompactDisplayWidget} 承载，见该类的分组说明。新增单行表单类 Widget 时
 * 继承 {@link CompactDisplayWidget} 即可拿到本配置。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/20
 */
public class CompactDisplayConfig implements Describable<CompactDisplayConfig>, Serializable {

    private static final long serialVersionUID = 1L;

    /** 宽度策略（与 {@link WidgetDisplayConfig#width} 逐字同名同序） */
    @FormField(ordinal = 0)
    public SizingMode width;

    /** 条件可见性：引用变量控制该 Widget 的显示隐藏（与 {@link WidgetDisplayConfig#conditionalVisibility} 同） */
    @FormField(ordinal = 2)
    public ConditionalVisibility conditionalVisibility;

    @TISExtension
    public static class DefaultDescriptor extends Descriptor<CompactDisplayConfig> {
        @Override
        public String getDisplayName() {
            return "Compact Widget Display Config";
        }
    }
}
