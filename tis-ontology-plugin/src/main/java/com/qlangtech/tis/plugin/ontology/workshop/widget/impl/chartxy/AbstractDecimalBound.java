package com.qlangtech.tis.plugin.ontology.workshop.widget.impl.chartxy;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;

import java.io.Serializable;

/**
 * 数值轴边界（上界 / 下界）配置的抽象基类（off / on 两态多态）。
 *
 * <p>原先上下界各由<b>两个平铺字段</b>表达：「是否自动计算」布尔 + 「边界值」Double：
 * <ul>
 *   <li>{@code autoMinBound} + {@code minBound}（下界）</li>
 *   <li>{@code autoMaxBound} + {@code maxBound}（上界）</li>
 * </ul>
 * 两个真值来源能造出「自动计算开着、却还留着一个手工值」的落盘状态 —— 那个手工值永远
 * 不生效，用户却以为生效了。改为多态后，值只存在于「手工指定」这一态里，矛盾状态在
 * 类型层面不可表示：
 * <ul>
 *   <li>{@link DecimalBoundOff} —— 零字段，表示<b>自动计算</b>（沿用改造前的默认）；</li>
 *   <li>{@link DecimalBoundConfig} —— 手工指定，边界值 {@code value} 必填。</li>
 * </ul>
 *
 * <p>同一个类型被 {@code ContinuousAxisConfig} 的 {@code minBound} 与 {@code maxBound}
 * 两个字段复用：两边只差「是上界还是下界」，形态完全一致，各建一套只会让同一个概念出现
 * 两个名字。
 *
 * <p>本类零 {@code @FormField}，因此不需要 {@code .json}。形态对齐同包的
 * {@link AbstractAxisTitleConfig}、{@link AbstractNumericFormatting}。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/25
 * @see DecimalBoundOff
 * @see DecimalBoundConfig
 */
public abstract class AbstractDecimalBound
        implements Describable<AbstractDecimalBound>, Serializable {

    private static final long serialVersionUID = 1L;

    public abstract static class BasicDescriptor extends Descriptor<AbstractDecimalBound> {
    }
}
