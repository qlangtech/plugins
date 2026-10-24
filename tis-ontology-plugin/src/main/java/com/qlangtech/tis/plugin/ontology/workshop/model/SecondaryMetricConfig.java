package com.qlangtech.tis.plugin.ontology.workshop.model;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.workshop.enums.VariableType;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.WidgetOptionHelper;

import java.io.Serializable;

/**
 * 副指标配置（如同比 / 环比）。
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
public class SecondaryMetricConfig implements Describable<SecondaryMetricConfig>, Serializable {

    private static final long serialVersionUID = 1L;

    /** 副指标标签；留空时前端不显示标签，只显示数值 */
    @FormField(ordinal = 0, type = FormFieldType.INPUTTEXT)
    public String label;

    @FormField(ordinal = 1, type = FormFieldType.ENUM, validate = {Validator.require})
    public MetricConfig.ValueType valueType = MetricConfig.ValueType.NUMBER;

    @FormField(ordinal = 2, type = FormFieldType.SELECTABLE, validate = {Validator.require})
    public String valueVariable;

    @FormField(ordinal = 3)
    public NumericFormatting numericFormatting;

    @TISExtension
    public static class DefaultDescriptor extends Descriptor<SecondaryMetricConfig> {

        public DefaultDescriptor() {
            super();
            this.registerSelectOptions(MetricConfig.KEY_VALUE_VARIABLE,
                    () -> WidgetOptionHelper.getVariableOptions(VariableType.STRING, VariableType.NUMERIC));
        }

        @Override
        public String getDisplayName() {
            return "Secondary Metric Config";
        }
    }
}
