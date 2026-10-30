package com.qlangtech.tis.plugin.ontology.workshop.widget.impl.chartxy;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;

/**
 * 柱状图画法。
 *
 * <p>柱状图支持在柱子上直接标注数值，故带 {@link #showLabels}；面积选项属于折线，
 * 不在本子类里。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/24
 */
public class BarLayerType extends AbstractLayerType {

    private static final long serialVersionUID = 1L;

    /**
     * 是否在柱子上显示数值标签。
     */
    @FormField(ordinal = 0,type = FormFieldType.ENUM)
    public Boolean showLabels = false;

    /**
     * 柱状图是唯一支持横向的画法（横向时类目轴在 y、数值轴在 x）。
     *
     * <p>见 {@link AbstractLayerType#supportsHorizontal()} 与
     * {@code ChartXYWidget#orientation}：折线/散点没有「柱子方向」可言，且在同一对共享
     * 坐标轴下无法与横向柱共存。
     */
    @Override
    public boolean supportsHorizontal() {
        return true;
    }

    @TISExtension
    public static class DefaultDescriptor extends BasicDescriptor implements DescriptorUseableShortComment {
        @Override
        public String getDisplayName() {
            return "Bar";
        }

        @Override
        public EndType getEndType() {
            return EndType.ChartBar;
        }

        @Override
        public String shortComment() {
            return "柱状图";
        }
    }
}
