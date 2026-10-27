package com.qlangtech.tis.plugin.ontology.workshop.model;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;

import java.io.Serializable;

/**
 * 时间序列可视化配置的抽象基类（支持多态）。
 *
 * <p>宿主 {@link CardLayout#visualization} 原先是一个开关（{@code MetricConfig.showVisualization}）
 * 加一个可空的具体类字段：开关与配置对象是两个独立的真值来源，能造出「开关开了却没配」
 * 「配了但开关关了」这类自相矛盾的状态。现在改为抽象 Describable：
 * <ul>
 *   <li>{@link TimeSeriesVisualizationOff} —— 零字段，表示不显示时间序列图表；</li>
 *   <li>{@link TimeSeriesVisualization} —— 启用实现，位置 / 时间序列集 / 时间范围 / 基线。</li>
 * </ul>
 *
 * <p>字段挂在 {@link CardLayout} 而不是 {@link MetricConfig} 上：该可视化只在卡片布局下支持，
 * 挂在指标上会让标签 / 列表布局也携带一个永不渲染的配置。
 *
 * <p>本类<b>刻意不声明任何 {@code @FormField} 字段</b>：字段全在子类里。ordinal 在同一次
 * {@code PropertyType.buildPropertyTypes} 里排序，基类与子类各声明一份会撞号。
 * 也因此本类不需要自己的 {@code .json}。
 *
 * <p>同一取舍见 {@link ConditionalVisibilityConfig}、{@link AbstractSecondaryMetricConfig}。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/24
 */
public abstract class AbstractTimeSeriesVisualization
        implements Describable<AbstractTimeSeriesVisualization>, Serializable {

    private static final long serialVersionUID = 1L;

    public abstract static class BasicDescriptor extends Descriptor<AbstractTimeSeriesVisualization> {
    }
}
