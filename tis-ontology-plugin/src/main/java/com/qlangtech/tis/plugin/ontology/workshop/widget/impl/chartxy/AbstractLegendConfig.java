package com.qlangtech.tis.plugin.ontology.workshop.widget.impl.chartxy;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;

import java.io.Serializable;

/**
 * 图例配置的抽象基类（off / on 两态多态）。
 *
 * <p>宿主 {@code ChartXYWidget.showLegend} 原先声明为 {@code Boolean}，图例位置则是另一个
 * 平铺字段 {@code legendPosition} —— 开关没开时位置字段照样渲染，且「关了还留着位置」这种
 * 自相矛盾的状态在落盘里是合法的。改成抽象 Describable 后：
 * <ul>
 *   <li>{@link LegendOff} —— 零字段，表示不显示图例；</li>
 *   <li>{@link LegendConfig} —— 显示图例，位置是本子表单专有的字段。</li>
 * </ul>
 *
 * <p>本类<b>刻意不声明任何 {@code @FormField} 字段</b>：字段全在子类里。ordinal 在同一次
 * {@code PropertyType.buildPropertyTypes} 里排序，基类与子类各声明一份会撞号。也因此本类
 * 不需要自己的 {@code .json}。
 *
 * <p>形态对齐 {@code AbstractSecondaryMetricConfig}。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/24
 */
public abstract class AbstractLegendConfig
        implements Describable<AbstractLegendConfig>, Serializable {

    private static final long serialVersionUID = 1L;

    public abstract static class BasicDescriptor extends Descriptor<AbstractLegendConfig> {
    }
}
