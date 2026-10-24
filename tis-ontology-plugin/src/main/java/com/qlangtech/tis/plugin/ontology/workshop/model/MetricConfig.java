package com.qlangtech.tis.plugin.ontology.workshop.model;

import com.google.common.collect.Lists;
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.IPluginStore;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.workshop.enums.VariableType;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.WidgetOptionHelper;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 单个指标配置 —— 指标卡里一个指标就是一行的元素（{@code MULTI_DESCRIBLE_PLUGIN}）。
 *
 * <p>{@code label} 是子表单行的 identity：它既是用户可见的指标标题，也是行在落盘/回填时的定位键，
 * 因此设为必填。刻意不加 {@link Validator#identity} —— 它的字符集限制会把中文标签直接拒掉。
 *
 * <p><b>本类不得出现类型为 {@code MetricConfig}（或经任何路径绕回 {@code MetricConfig}）的
 * {@code @FormField} 字段。</b> {@code DescriptorsJSON} 遍历嵌套 Describable 时没有环检测、
 * 也没有深度上限，自嵌套会让指标卡的表单一打开就 StackOverflowError。
 * 副指标用<b>另一个类型</b> {@link SecondaryMetricConfig} 承载（字段结构是它的子集，
 * 但它不再含副指标字段，故不构成环）。
 *
 * @author 百岁 (baisui@qlangtech.com)
 */
public class MetricConfig implements Describable<MetricConfig>, Serializable,
        IPluginStore.MultiDescribleElement {

    private static final long serialVersionUID = 1L;

    public static final String KEY_VALUE_TYPE = "valueType";
    public static final String KEY_VALUE_VARIABLE = "valueVariable";

    /** 指标标签；同时是子表单行的 identity */
    @FormField(identity = true, ordinal = 0, type = FormFieldType.INPUTTEXT,
            validate = {Validator.require})
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
     * 条件格式化规则，由子表单结构化管理，按列表顺序求值、首条命中生效。
     */
    @FormField(ordinal = 5, type = FormFieldType.MULTI_DESCRIBLE_PLUGIN, desClazz = ConditionalFormatRule.class)
    public List<ConditionalFormatRule> conditionalFormatting = new ArrayList<>();

    @FormField(ordinal = 6, type = FormFieldType.ENUM)
    public Boolean showVisualization = false;

    @FormField(ordinal = 7)
    public TimeSeriesVisualization visualization;

    /** 是否展示副指标（如同比 / 环比）；关闭时即使配置了也不渲染 */
    @FormField(ordinal = 8, type = FormFieldType.ENUM)
    public Boolean showSecondaryMetric = false;

    /**
     * 副指标配置。
     * <p>
     * 类型是 {@link SecondaryMetricConfig} 而不是本类 —— 见类注释里「不得自引用」的约束。
     */
    @FormField(ordinal = 9)
    public SecondaryMetricConfig secondaryMetric;

    /**
     * 指标间的显示顺序。子表单支持拖拽排序（见描述符的 {@code isEnableRowDragDrop}），
     * 前端读取时按本字段升序排序。
     */
    @FormField(ordinal = 10, type = FormFieldType.INT_NUMBER, advance = true)
    public Integer order = 0;

    @Override
    public String identityValue() {
        return this.label;
    }

    @TISExtension
    public static class DefaultDescriptor extends IPluginStore.MultiDescribleElementDescriptor<MetricConfig> {

        public DefaultDescriptor() {
            super();
            this.registerSelectOptions(KEY_VALUE_VARIABLE,
                    () -> WidgetOptionHelper.getVariableOptions(VariableType.STRING, VariableType.NUMERIC));
        }

        @Override
        public String getDisplayName() {
            return "Metric Config";
        }

        @Override
        public String shortComment() {
            return "指标";
        }

        @Override
        public ViewStyle viewStyle() {
            return ViewStyle.Table;
        }

        @Override
        public List<ColConfig> colsConfig() {
            // width 是百分比（见前端 MultiChildViewDescriptor.colWidth），三列合计 100
            return Lists.newArrayList(
                    new ColConfig("label", 30).setClickable(),
                    new ColConfig("valueVariable", 40),
                    new ColConfig("valueType", 30));
        }

        @Override
        protected RowDragDropCfg isEnableRowDragDrop() {
            // 指标的展示顺序由用户拖拽决定
            return new RowDragDropCfg(true, "order");
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
