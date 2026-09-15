package com.qlangtech.tis.plugin.ontology.workshop.model;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;

/**
 * 连续轴配置（Continuous Axis / Value Axis）
 *
 * <p>轴上绘制的是<b>连续数值</b>（聚合结果），因此需要刻度类型与上下界。
 * 与之相对的是 {@link CategoricalAxisConfig}。
 *
 * <h3>关于 {@code TIME} 刻度</h3>
 * 旧实现把 {@code LINEAR / LOG / TIME} 并列成「刻度类型」，这是类别错误：
 * <b>时间不是刻度类型，而是轴属性的数据类型</b>。时间轴由
 * {@link ChartLayer#xAxisProperty} 指向 timestamp 属性 + 运行期时间桶粒度决定，
 * 与刻度类型正交。因此本枚举只保留官方文档承认的线性与对数两种。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/15
 */
public class ContinuousAxisConfig extends AxisConfig {

    private static final long serialVersionUID = 1L;

    /**
     * 刻度类型；默认线性。
     */
    @FormField(ordinal = 10, type = FormFieldType.ENUM)
    public AxisScale scaleType = AxisScale.LINEAR;

    /**
     * 是否按 series 分别配置值轴。
     *
     * <p>仅在同一图层/图表存在多个 series、且各 series 数量级差异悬殊时才有意义。
     */
    @FormField(ordinal = 11, type = FormFieldType.ENUM, advance = true)
    public Boolean useMultipleAxes = false;

    /**
     * 是否自动计算下界；默认自动。
     */
    @FormField(ordinal = 12, type = FormFieldType.ENUM)
    public Boolean autoMinBound = true;

    /**
     * 下界值；仅 {@link #autoMinBound} 关闭时生效。
     */
    @FormField(ordinal = 13, type = FormFieldType.INT_NUMBER, advance = true)
    public Double minBound;

    /**
     * 是否自动计算上界；默认自动。
     */
    @FormField(ordinal = 14, type = FormFieldType.ENUM)
    public Boolean autoMaxBound = true;

    /**
     * 上界值；仅 {@link #autoMaxBound} 关闭时生效。
     */
    @FormField(ordinal = 15, type = FormFieldType.INT_NUMBER, advance = true)
    public Double maxBound;

    /**
     * 刻度类型。仅线性与对数两种 —— 时间轴不是刻度类型，见类注释。
     */
    public enum AxisScale implements DescriptorUseableShortComment {
        LINEAR("线性（默认）"),
        LOG("对数");

        private final String comment;

        AxisScale(String comment) {
            this.comment = comment;
        }

        @Override
        public String shortComment() {
            return comment;
        }
    }

    @TISExtension
    public static class DefaultDescriptor extends BasicDescriptor implements DescriptorUseableShortComment {

        @Override
        public String getDisplayName() {
            return "Continuous Axis";
        }

        @Override
        public String shortComment() {
            return "连续轴";
        }
    }
}
