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

import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.ontology.workshop.enums.WorkshopCptType;
import com.qlangtech.tis.plugin.ontology.workshop.enums.WorkshopCptTypeGetter;

/**
 * 精简显示配置：只含<b>宽度</b>。
 *
 * <h3>与 {@link WidgetDisplayConfig} 的关系</h3>
 * 本类是 {@link WidgetDisplayConfig} 的<b>同名字段子集</b>：{@code width} 字段名、类型、
 * ordinal 逐一相同（都是 {@code 1}，便于读者对照），因此前端读
 * {@code widget.displayConfig.vals.width} 对两个 impl 走同一条路径。
 *
 * <p>哪些 Widget 用本 impl 由 {@link AbstractWidgetDisplayConfig#configClazzOf} 决定
 * （即 {@code WorkShopWidgetType.fullDisplayWidget == false}），用户不能选。
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
 * 让这些字段出现在表单里，用户会填了却发现没有任何效果。
 *
 * <p>同理，宽度槽位在自适应下也<b>不该</b>冒出高度概念：{@link AutoSizingMode} 的上限字段
 * 叫 {@code maxSize} 而非 {@code maxHeight}，正是为了让「宽度 → 自适应」下的那个输入框
 * 不对着一行的宽度谈高度。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/20
 */
public class CompactDisplayConfig extends AbstractWidgetDisplayConfig {

    private static final long serialVersionUID = 1L;

    /**
     * 宽度策略（与 {@link WidgetDisplayConfig#width} 逐字同名同序）
     */
    @FormField(ordinal = 1)
    public SizingMode width;

    @TISExtension
    public static class DefaultDescriptor extends Descriptor<CompactDisplayConfig> implements WorkshopCptTypeGetter {
        @Override
        public String getDisplayName() {
            return "精简显示配置";
        }

        @Override
        public WorkshopCptType getWorkshopCptType() {
            return WorkshopCptType.WidgetDisplayConfig;
        }
    }
}
