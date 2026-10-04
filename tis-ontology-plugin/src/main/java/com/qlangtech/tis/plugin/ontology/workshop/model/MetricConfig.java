package com.qlangtech.tis.plugin.ontology.workshop.model;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.workshop.enums.VariableType;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.WidgetOptionHelper;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 单个指标配置。
 *
 * <p><b>本类不得再出现类型为 {@code MetricConfig}（或经任何路径绕回 {@code MetricConfig}）
 * 的 {@code @FormField} 字段。</b> 曾经的 {@code secondaryMetric : MetricConfig} 就是这种自嵌套：
 * {@code DescriptorsJSON} 遍历嵌套 Describable 时没有环检测、也没有深度上限
 * （{@code DefaultDescriptorsJSON.JSONAttrVal.putDescriptors} 立即递归求值），
 * 于是「主指标 → 副指标 → 副指标 → …」无限下钻，指标卡的表单直接 StackOverflowError。
 * 「主 + 副」这种同类配对关系请把两个字段<b>平级挂在宿主上</b>
 * （见 {@link com.qlangtech.tis.plugin.ontology.workshop.widget.impl.MetricCardWidget}
 * 的 {@code metric} 与 {@code secondaryMetric}），不要做成父子嵌套。
 */
public class MetricConfig implements Describable<MetricConfig>, Serializable {

    private static final long serialVersionUID = 1L;

    public static final String KEY_VALUE_TYPE = "valueType";
    public static final String KEY_VALUE_VARIABLE = "valueVariable";

    @FormField(ordinal = 0, type = FormFieldType.INPUTTEXT)
    public String label;

    @FormField(ordinal = 1, type = FormFieldType.TEXTAREA)
    public String description;

    @FormField(ordinal = 2, type = FormFieldType.ENUM, validate = {Validator.require})
    public ValueType valueType = ValueType.NUMBER;

    /**
     * 指标取值来源变量。
     * <p>
     * 选项按 {@link ValueType} 的两档（字符串 / 数值）供给 —— 两者都放进来，
     * 让「取了哪种变量」与「valueType 选了什么」能对得上，而不是靠使用者自己记住。
     */
    @FormField(ordinal = 3, type = FormFieldType.SELECTABLE, validate = {Validator.require})
    public String valueVariable; // 变量 ID

    @FormField(ordinal = 4)
    public NumericFormatting numericFormatting;

    /**
     * 条件着色规则，由子表单结构化管理。
     * <p>
     * 原先声明为 {@code MULTI_SELECTABLE} 且没有登记行编辑器，前端拿到的是一个填不进去的
     * 多选下拉；改为 {@code MULTI_DESCRIBLE_PLUGIN} 后由 {@link ConditionalFormatRule} 承担行结构。
     */
    @FormField(ordinal = 5, type = FormFieldType.MULTI_DESCRIBLE_PLUGIN, desClazz = ConditionalFormatRule.class)
    public List<ConditionalFormatRule> conditionalFormatting = new ArrayList<>();

    @FormField(ordinal = 6, type = FormFieldType.ENUM)
    public Boolean showVisualization = false;

    @FormField(ordinal = 7)
    public TimeSeriesVisualization visualization;

    @TISExtension
    public static class DefaultDescriptor extends Descriptor<MetricConfig> {

        public DefaultDescriptor() {
            super();
            this.registerSelectOptions(KEY_VALUE_VARIABLE,
                    () -> WidgetOptionHelper.getVariableOptions(VariableType.STRING, VariableType.NUMERIC));
        }

        @Override
        public String getDisplayName() {
            return "Metric Config";
        }
    }

    public enum ValueType implements DescriptorUseableShortComment {
        STRING("字符串"),
        NUMBER("数值");

        public final String label;

        ValueType(String label) {
            this.label = label;
        }

        @Override
        public String shortComment() {
            return this.label;
        }
    }
}
