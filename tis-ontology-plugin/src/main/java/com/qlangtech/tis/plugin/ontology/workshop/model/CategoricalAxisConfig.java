package com.qlangtech.tis.plugin.ontology.workshop.model;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;

/**
 * 分类轴配置（Categorical Axis）
 *
 * <p>轴上绘制的是<b>离散的分类键</b>（如告警类型、飞机型号），而非连续数值。
 * 除基类的显示项外，本类只管一件事：<b>这些分类键按什么顺序排列</b>。
 *
 * <p>与之相对的是 {@link ContinuousAxisConfig}。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/15
 */
public class CategoricalAxisConfig extends AxisConfig {

    private static final long serialVersionUID = 1L;

    /**
     * 分类键排序方式；默认按键升序（A→Z）。
     */
    @FormField(ordinal = 10, type = FormFieldType.ENUM)
    public CategoricalSortBy sortBy = CategoricalSortBy.KEY_ASCENDING;

    /**
     * 自定义排序所用的度量属性名；仅 {@link #sortBy} 为
     * {@link CategoricalSortBy#CUSTOM} 时生效。
     *
     * <p>官方语义：给属性赋予数值排序值，再选择该属性作为排序度量。
     */
    @FormField(ordinal = 11, type = FormFieldType.INPUTTEXT, advance = true)
    public String customSortMetric;

    /**
     * 分类键排序方式。
     *
     * <p>四个取值对齐官方文档与 {@code arch/05-workshop-architecture.md} 中的
     * {@code 'alphabetical' | 'value-asc' | 'value-desc' | 'custom'}。
     */
    public enum CategoricalSortBy implements DescriptorUseableShortComment {
        KEY_ASCENDING("按键升序（A→Z，默认）"),
        VALUE_ASCENDING("按值升序"),
        VALUE_DESCENDING("按值降序"),
        CUSTOM("自定义排序");

        private final String comment;

        CategoricalSortBy(String comment) {
            this.comment = comment;
        }

        @Override
        public String shortComment() {
            return comment;
        }
    }

    @TISExtension
    public static class DefaultDescriptor extends BasicDescriptor implements DescriptorUseableShortComment {

        @Override
        public String getDisplayName() {
            return "Categorical Axis";
        }

        @Override
        public String shortComment() {
            return "分类轴";
        }
    }
}
