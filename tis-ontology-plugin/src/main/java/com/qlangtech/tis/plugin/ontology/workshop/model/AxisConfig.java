package com.qlangtech.tis.plugin.ontology.workshop.model;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;

import java.io.Serializable;

/**
 * 坐标轴配置（抽象基类）
 *
 * <p>本类只承载<b>两种轴性质共有</b>的显示配置；轴性质专有的配置分别落在
 * {@link CategoricalAxisConfig}（分类轴，额外有排序方式）与 {@link ContinuousAxisConfig}
 * （连续轴，额外有刻度类型与上下界）。
 *
 * <h3>为什么是多态而不是「一个类 + kind 枚举」</h3>
 * 分类轴与连续轴的专有字段互不适用：分类轴没有「刻度类型」，连续轴没有「排序方式」。
 * 若压成单类加 {@code kind} 字段，两组字段会同时出现在表单里，用户必须自行判断哪些
 * 与自己选的轴性质有关。多态则让 TIS 原生的 impl 选择器先定轴性质、再渲染对应字段，
 * 形态对齐本模块既有先例 {@code VariableDefinitionConfig} 与 {@code OverlayTypeConfig}。
 *
 * <h3>轴绑定<b>不在</b>本类</h3>
 * 「轴上画什么」由 {@link ChartLayer#xAxisProperty} 与各 series 的聚合方式决定；
 * 本类只描述轴<b>长什么样</b>。原先 {@code AxisConfig} 上的 {@code variable}
 * （必填 INPUTTEXT）把数据来源混进了显示配置，已删除。
 *
 * <h3>字段序位</h3>
 * 基类占用 {@code 0..5}，各子类字段从 {@code 10} 起编排，避免 ordinal 撞车
 * —— TIS 的表单排序是对 {@code formField.ordinal()} 做稳定排序，而字段集合来自
 * HashMap，相同 ordinal 的相对顺序不确定。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/15
 * @see CategoricalAxisConfig
 * @see ContinuousAxisConfig
 */
public abstract class AxisConfig implements Describable<AxisConfig>, Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 是否显示轴标题；开启后 {@link #titleOverride} 才生效。
     * 默认标题取该轴上所绘的聚合方式 / 属性名。
     */
    @FormField(ordinal = 0, type = FormFieldType.ENUM)
    public Boolean showTitle = false;

    /**
     * 轴标题覆写文本；留空则使用自动生成的标题。
     */
    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, advance = true)
    public String titleOverride;

    /**
     * 是否启用数值格式化（数值分组、小数位、科学计数等）。
     */
    @FormField(ordinal = 2, type = FormFieldType.ENUM, advance = true)
    public Boolean enableNumericalFormatting = false;

    /**
     * 数值格式化细则；仅 {@link #enableNumericalFormatting} 开启时生效。
     */
    @FormField(ordinal = 3, advance = true)
    public NumericFormatting numericFormatting;

    /**
     * 是否显示网格线。
     */
    @FormField(ordinal = 4, type = FormFieldType.ENUM)
    public Boolean showGridlines = false;

    /**
     * 是否显示颜色标记（轴上的系列配色圆点）。
     */
    @FormField(ordinal = 5, type = FormFieldType.ENUM, advance = true)
    public Boolean showColorMarkers = false;

    /**
     * 轴配置 descriptor 基类。
     *
     * <p>可见性为 {@code public}：{@code ChartXYWidget} 的表单侧需要引用各子类
     * descriptor，protected 跨包不可见。形态对齐 TIS 中同类先例
     * {@code ValueConstraint.BaseDesc} 与本模块的 {@code VariableDefinitionConfig.BasicDescriptor}。
     */
    public abstract static class BasicDescriptor extends Descriptor<AxisConfig> {
    }
}
