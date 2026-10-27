package com.qlangtech.tis.plugin.ontology.workshop.widget.impl.chartxy;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;

import java.io.Serializable;

/**
 * 数值格式化配置的抽象基类（off / on 两态多态）。
 *
 * <p>原先「是否启用」由一个独立布尔字段 {@code enableNumericalFormatting} 承担，格式化
 * 细则则平铺在旁边 —— 两个真值来源，能造出「开关关了细则还在」的状态。改为多态：
 * <ul>
 *   <li>{@link NumericFormattingOff} —— 零字段，不做格式化；</li>
 *   <li>{@code model.NumericFormatting} —— 启用实现，承载分组 / 小数位 / 记数法 / 单位。</li>
 * </ul>
 *
 * <p>on 子类<b>保留在 model 包</b>并沿用原类名：它对 {@code MetricConfig} /
 * {@code SecondaryMetricConfig} 同样是共享类型，且已被多处引用。
 *
 * <p>本类零 {@code @FormField}，因此不需要 {@code .json}。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/24
 */
public abstract class AbstractNumericFormatting
        implements Describable<AbstractNumericFormatting>, Serializable {

    private static final long serialVersionUID = 1L;

    public abstract static class BasicDescriptor extends Descriptor<AbstractNumericFormatting> {
    }
}
