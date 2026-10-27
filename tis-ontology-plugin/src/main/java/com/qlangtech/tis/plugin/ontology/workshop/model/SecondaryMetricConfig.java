package com.qlangtech.tis.plugin.ontology.workshop.model;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.workshop.enums.VariableType;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.WidgetOptionHelper;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.chartxy.AbstractNumericFormatting;

/**
 * 副指标配置（启用实现，如同比 / 环比）。
 *
 * <p>多态体系见父类 {@link AbstractSecondaryMetricConfig}：「不显示」由零字段的子类
 * {@link SecondaryMetricOff} 表达；本类只在用户显式选择启用时出现，因此
 * {@link #valueType} 与 {@link #valueVariable} 都带 {@code Validator.require}。
 *
 * <p>为什么单独一个类型、而不是让 {@link MetricConfig} 自嵌套：后者会让
 * {@code DescriptorsJSON} 的递归下钻无限进行（无环检测 / 无深度上限），指标卡表单直接
 * StackOverflowError。本类是 {@link MetricConfig} 的字段子集，且<b>不含</b>任何指回
 * {@code MetricConfig} 的字段，故不构成环。
 *
 * <p>副指标只有「取值 + 展示格式」，不承载条件格式化 —— 条件规则针对的是主指标值。
 *
 * @author 百岁 (baisui@qlangtech.com)
 */
public class SecondaryMetricConfig extends AbstractSecondaryMetricConfig {

    private static final long serialVersionUID = 1L;

    /** 副指标标签；留空时前端不显示标签，只显示数值 */
    @FormField(ordinal = 0, type = FormFieldType.INPUTTEXT)
    public String label;

    @FormField(ordinal = 1, type = FormFieldType.ENUM, validate = {Validator.require})
    public MetricConfig.ValueType valueType = MetricConfig.ValueType.NUMBER;

    @FormField(ordinal = 2, type = FormFieldType.SELECTABLE, validate = {Validator.require})
    public String valueVariable;

    /**
     * 数值格式化（off / 启用两态多态）。
     */
    @FormField(ordinal = 3, validate = {Validator.require})
    public AbstractNumericFormatting numericFormatting = new NumericFormatting();

    /**
     * 空值回填。既由本类的 {@code readResolve()} 调用，也由
     * {@link MetricConfig#readResolve()} 兜一次 —— 后者覆盖「实例并非从 XML 读出、
     * 但字段被显式置空」的路径（如表单逐字段回填）。
     */
    public void normalize() {
        if (this.numericFormatting == null) {
            this.numericFormatting = new NumericFormatting();
        }
    }

    private Object readResolve() {
        normalize();
        return this;
    }

    @TISExtension
    public static class DefaultDescriptor extends BasicDescriptor implements DescriptorUseableShortComment {

        public DefaultDescriptor() {
            super();
            this.registerSelectOptions(MetricConfig.KEY_VALUE_VARIABLE,
                    () -> WidgetOptionHelper.getVariableOptions(VariableType.STRING, VariableType.NUMERIC));
        }

        @Override
        public String getDisplayName() {
            return "Secondary Metric Config";
        }

        @Override
        public String shortComment() {
            return "副指标（取值变量与数值格式）";
        }
    }
}
