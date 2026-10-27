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
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.chartxy.AbstractNumericFormatting;

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
 * 副指标用<b>另一个类型</b> {@link AbstractSecondaryMetricConfig} 承载（字段结构是它的子集，
 * 但它不再含副指标字段，故不构成环）。
 *
 * <p>副指标改为 off / 启用两态多态（见 {@link AbstractSecondaryMetricConfig}），不再有
 * {@code showSecondaryMetric} 布尔开关。时间序列可视化只在卡片布局下支持，已下移到
 * {@link CardLayout#visualization}，本类不再承载。
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

    /**
     * 数值格式化（off / 启用两态多态）。
     */
    @FormField(ordinal = 4, validate = {Validator.require})
    public AbstractNumericFormatting numericFormatting = new NumericFormatting();

    /**
     * 条件格式化规则，由子表单结构化管理，按列表顺序求值、首条命中生效。
     */
    @FormField(ordinal = 5, type = FormFieldType.MULTI_DESCRIBLE_PLUGIN, desClazz = ConditionalFormatRule.class)
    public List<ConditionalFormatRule> conditionalFormatting = new ArrayList<>();

    // ordinal 6 / 7 留空：原 showVisualization / visualization 已下移到 CardLayout.visualization
    // ordinal 8 留空：原 showSecondaryMetric 布尔开关已由 secondaryMetric 的 off 子类取代
    // （按 WorkshopWidget 既有约定，删字段后序位留空、不重排）

    /**
     * 副指标配置（off / 启用两态多态，见 {@link AbstractSecondaryMetricConfig}）。
     * <p>
     * 类型是 {@link AbstractSecondaryMetricConfig} 而不是本类 —— 见类注释里「不得自引用」的约束。
     */
    @FormField(ordinal = 9, validate = {Validator.require})
    public AbstractSecondaryMetricConfig secondaryMetric = new SecondaryMetricOff();

    /**
     * 指标间的显示顺序。子表单支持拖拽排序（见描述符的 {@code isEnableRowDragDrop}），
     * 前端读取时按本字段升序排序。
     */
    @FormField(ordinal = 10,type = FormFieldType.INT_NUMBER)
    public Integer order = 0;

    /**
     * XStream 反序列化不跑构造器与字段初值：早于本次改造落盘的指标行里
     * {@code secondaryMetric} 可能整个缺失，补成 off，避免前端渲染时 NPE。
     */
    private Object readResolve() {
        if (this.secondaryMetric == null) {
            this.secondaryMetric = new SecondaryMetricOff();
        }
        if (this.secondaryMetric instanceof SecondaryMetricConfig secondaryMetricConfig) {
            secondaryMetricConfig.normalize();
        }
        if (this.conditionalFormatting == null) {
            this.conditionalFormatting = new ArrayList<>();
        }
        this.numericFormatting = this.numericFormatting == null
                ? new NumericFormatting() : this.numericFormatting;
        return this;
    }

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
