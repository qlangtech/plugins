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
import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.ontology.workshop.enums.WorkshopCptType;
import com.qlangtech.tis.plugin.ontology.workshop.enums.WorkshopCptTypeGetter;

/**
 * 完整显示配置：宽度 / 高度 / 展示优化。
 *
 * <h3>与 {@link CompactDisplayConfig} 的分工</h3>
 * 本 impl 面向<b>有内容区</b>的 Widget（表格、图表、看板卡片、Markdown 正文…），内容长短不定，
 * 高度与挂载策略都说得通。单行表单控件（日期选择器、输入框、复选框…）与页头文字、按钮组
 * 用不了高度与挂载策略，对应 {@link CompactDisplayConfig}。
 * <p>
 * 走哪一个<b>由 Widget 类型决定</b>，不是用户的选项：判据单点在
 * {@link AbstractWidgetDisplayConfig#configClazzOf(com.qlangtech.tis.plugin.workshop.widget.IWorkshopWidget)}，
 * 数据源是 {@code WorkShopWidgetType.fullDisplayWidget}（下发为 {@code extractProps.fullDisplay}）。
 * 前端 Display tab 只呈现对应那份表单，没有 impl 单选。
 *
 * <p>「条件可见性」不在本类，已上移到 {@link WorkshopWidget#conditionalVisibility}
 * （Palantir 把可见性归在 Widget setup，不属于 Display）。
 *
 * <h3>字段形状</h3>
 * <ul>
 *   <li>{@link #width} / {@link #height} —— 结构化多态 {@link SizingMode}（自适应 / 固定像素 /
 *       flex 比例），字段形状以前端为准：自由文本要靠前端解析字符串，既无法校验也
 *       表达不了「按比例分配」。</li>
 *   <li>{@link #displayOptimization} —— 由原来的 {@code lazyLoading} + {@code virtualScroll}
 *       两个 Boolean 合并而来。两个布尔有 4 种组合，真正有意义的只有 3 种，且原来的组合
 *       <em>表达不了</em>前端的 {@code never-unmount}（永不卸载）。</li>
 * </ul>
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/13
 */
public class WidgetDisplayConfig extends AbstractWidgetDisplayConfig {

    private static final long serialVersionUID = 1L;

    /** 宽度策略 */
    @FormField(ordinal = 1)
    public SizingMode width;

    /** 高度策略 */
    @FormField(ordinal = 2)
    public SizingMode height;

    /** 挂载 / 渲染优化策略 */
    @FormField(ordinal = 3, type = FormFieldType.ENUM)
    public DisplayOptimization displayOptimization = DisplayOptimization.dft;

    /**
     * 展示优化策略。
     * <p>
     * Java 枚举常量名无法直接写成前端的字面量（{@code default} 是保留字、kebab-case 也不是合法标识符），
     * 因此用 {@link #key} 显式声明与前端 {@code WidgetDisplay.displayOptimization} 的映射。
     */
    public enum DisplayOptimization implements DescriptorUseableShortComment {
        /** 默认：进入视口即挂载，离开后卸载 */
        dft("default", "默认"),
        /** 提前挂载：即便不在视口也预先挂载，切换更快 */
        eagerMount("eager-mount", "提前挂载"),
        /** 永不卸载：挂载后常驻，保留内部状态（如滚动位置） */
        neverUnmount("never-unmount", "永不卸载");

        /** 与前端字面量一致的 key */
        public final String key;
        public final String label;

        DisplayOptimization(String key, String label) {
            this.key = key;
            this.label = label;
        }

        @Override
        public String shortComment() {
            return label;
        }
    }

    @TISExtension
    public static class DefaultDescriptor extends Descriptor<WidgetDisplayConfig> implements WorkshopCptTypeGetter {
        @Override
        public String getDisplayName() {
            return "完整显示配置";
        }

        @Override
        public WorkshopCptType getWorkshopCptType() {
            return WorkshopCptType.WidgetDisplayConfig;
        }
    }
}
