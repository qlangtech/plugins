package com.qlangtech.tis.plugin.ontology.workshop.model;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;

import java.io.Serializable;

/**
 * 副指标配置的抽象基类（支持多态）。
 *
 * <p>宿主 {@link MetricConfig#secondaryMetric} 原先声明为具体类 {@link SecondaryMetricConfig}，
 * 「不显示副指标」只能靠另一个布尔字段 {@code showSecondaryMetric} 表达 —— 开关与配置对象是
 * 两个独立的真值来源，能造出「开关开了却没配」「配了但开关关了」这类自相矛盾的状态。
 * 现在改为抽象 Describable：
 * <ul>
 *   <li>{@link SecondaryMetricOff} —— 零字段，表示不显示副指标；</li>
 *   <li>{@link SecondaryMetricConfig} —— 启用实现，副指标的标签 / 取值变量 / 数值格式。</li>
 * </ul>
 *
 * <p>本类<b>刻意不声明任何 {@code @FormField} 字段</b>：字段全在子类里。ordinal 在同一次
 * {@code PropertyType.buildPropertyTypes} 里排序，基类与子类各声明一份会撞号。
 * 也因此本类不需要自己的 {@code .json}。
 *
 * <p>同一取舍见 {@link ConditionalVisibilityConfig}、{@code model/section/HeaderFormat}。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/24
 */
public abstract class AbstractSecondaryMetricConfig
        implements Describable<AbstractSecondaryMetricConfig>, Serializable {

    private static final long serialVersionUID = 1L;

    public abstract static class BasicDescriptor extends Descriptor<AbstractSecondaryMetricConfig> {
    }
}
