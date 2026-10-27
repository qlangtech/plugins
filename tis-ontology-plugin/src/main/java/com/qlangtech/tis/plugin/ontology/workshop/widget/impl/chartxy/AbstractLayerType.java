package com.qlangtech.tis.plugin.ontology.workshop.widget.impl.chartxy;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.plugin.IEndTypeGetter;

import java.io.Serializable;

/**
 * 图层画法的抽象基类。
 *
 * <p>原先是 {@code ChartLayer} 上的 {@code LayerType} 枚举 + 两个只在特定画法下才有意义的
 * 平铺字段（{@code areaOption} 仅折线、{@code showLabels} 仅柱/线）。枚举表达不了
 * 「哪个字段属于哪种画法」，于是散点图上也会显示「面积选项」。改为多态：
 * <ul>
 *   <li>{@link BarLayerType} —— 柱状图，带 {@code showLabels}；</li>
 *   <li>{@link LineLayerType} —— 折线图，带 {@code areaOption} 与 {@code showLabels}；</li>
 *   <li>{@link ScatterLayerType} —— 散点图，零字段。</li>
 * </ul>
 *
 * <p>本类零 {@code @FormField}：字段在各自子类里，ordinal 才不会撞号；因此也不需要
 * 自己的 {@code .json}。形态对齐 {@code AbstractSecondaryMetricConfig}。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/24
 */
public abstract class AbstractLayerType
        implements Describable<AbstractLayerType>, Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 本画法是否支持「横向」—— 即把索引轴（后端分桶出来的类目/时间标签）放到屏幕的 y 方向。
     *
     * <p><b>为什么只有柱状图支持</b>：整张图共用一对 x/y scale
     * （{@code ChartXYWidget#xAxis} / {@code #yAxis}）。横向柱要求 y=类目轴、x=数值轴，
     * 而折线/散点的数据始终按「labels → x 轴、聚合值 → y 轴」解析（Chart.js 的
     * {@code indexAxis} 只是柱状 dataset 的选项，line/scatter 控制器不认它），
     * 所以同一对轴无法同时容纳横向柱和纵向折线/散点。
     *
     * <p>这是「画法的能力」，不是「方向取值」：方向属于整张图（{@code ChartXYWidget#orientation}），
     * 本方法只供其校验使用。完整规则与原因见 {@code ChartXYWidget#orientation} 的字段注释，
     * 修改前务必先读。
     */
    public boolean supportsHorizontal() {
        return false;
    }

    public abstract static class BasicDescriptor extends Descriptor<AbstractLayerType> implements IEndTypeGetter {
    }
}
