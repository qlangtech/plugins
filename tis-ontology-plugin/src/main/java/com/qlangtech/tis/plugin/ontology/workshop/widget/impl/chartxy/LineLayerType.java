package com.qlangtech.tis.plugin.ontology.workshop.widget.impl.chartxy;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;

/**
 * 折线图画法。
 *
 * <p>折线图专有两项配置：是否把折线下方填成面积（{@link #areaOption}），以及是否在
 * 数据点上标注数值（{@link #showLabels}）。前者从 {@code ChartLayer} 的平铺字段下沉而来
 * —— 它只对折线有意义，散点图上出现这个选择器本身就是误导。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/24
 */
public class LineLayerType extends AbstractLayerType {

    private static final long serialVersionUID = 1L;

    /**
     * 面积选项；普通折线 / 面积 / 堆叠面积。
     */
    @FormField(ordinal = 0, type = FormFieldType.ENUM)
    public AreaOption areaOption = AreaOption.LINE;

    /**
     * 是否在数据点上显示数值标签。
     */
    @FormField(ordinal = 1, type = FormFieldType.ENUM)
    public Boolean showLabels = false;

    /**
     * 面积选项。
     */
    public enum AreaOption implements DescriptorUseableShortComment {
        LINE("普通折线"),
        AREA("面积（折线下方填充）"),
        STACKED("堆叠面积（分段值上下堆叠）");

        private final String comment;

        AreaOption(String comment) {
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
            return "Line";
        }

        @Override
        public EndType getEndType() {
            return EndType.ChartLine;
        }

        @Override
        public String shortComment() {
            return "折线图";
        }
    }
}
