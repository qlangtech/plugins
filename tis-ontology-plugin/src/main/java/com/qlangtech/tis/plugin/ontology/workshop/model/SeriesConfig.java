package com.qlangtech.tis.plugin.ontology.workshop.model;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;

import java.io.Serializable;

/**
 * 图表系列配置（Chart Series）
 *
 * <p>系列是<b>图层内部</b>按维度/度量做的次级区分。一个图层可以画多个系列，例如
 * X 轴为「告警类型」时，同时画「各类型的告警数量」与「各类型的延误小时数之和」。
 *
 * <h3>画法不在本类</h3>
 * 「画成柱状还是折线」属于图层（{@link ChartLayer#layerType}），不属于系列：
 * 同一图层内的多个系列共用同一种画法，否则坐标系无法自洽。
 * 旧实现把 {@code chartType} 放在系列上，已上移至图层。
 *
 * <h3>与图层的关系</h3>
 * <pre>
 *   {@link ChartLayer}    ：数据从哪来、画成什么形状、X 轴按什么分组
 *     └─ SeriesConfig     ：本层内按哪个度量聚合、是否二次分段、配色与图例名（本类）
 * </pre>
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/15
 */
public class SeriesConfig implements Describable<SeriesConfig>, Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 系列名称（图例中显示的默认名称）。
     */
    @FormField(ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String name;

    /**
     * 数据源变量名（提供本系列数据的变量）。
     */
    @FormField(ordinal = 1, type = FormFieldType.SELECTABLE, validate = {Validator.require})
    public String variable;

    /**
     * 聚合方式；默认计数。
     */
    @FormField(ordinal = 2, type = FormFieldType.ENUM, validate = {Validator.require})
    public AggregationType aggregation = AggregationType.COUNT;

    /**
     * 参与聚合的度量属性名；{@link #aggregation} 为 {@link AggregationType#COUNT}
     * 时不需要（计数不依赖具体属性）。
     */
    @FormField(ordinal = 3, type = FormFieldType.INPUTTEXT)
    public String property;

    /**
     * 二次分段属性（可选）—— 按该属性把每个值再拆开。
     */
    @FormField(ordinal = 4, type = FormFieldType.INPUTTEXT, advance = true)
    public String segmentBy;

    /**
     * 图例显示名覆盖（可选）；分段图表中可覆盖单个分段的名称。
     */
    @FormField(ordinal = 5, type = FormFieldType.INPUTTEXT, advance = true)
    public String displayOverride;

    /**
     * 系列配色（CSS 颜色值）。
     */
    @FormField(ordinal = 6, type = FormFieldType.INPUTTEXT, advance = true)
    public String color;

    /**
     * 空值显示方式；仅折线图有效。
     */
    @FormField(ordinal = 7, type = FormFieldType.ENUM, advance = true)
    public NullDisplay nullDisplay = NullDisplay.GAP;

    /**
     * 分段显示方式；仅柱状图有效。
     */
    @FormField(ordinal = 8, type = FormFieldType.ENUM, advance = true)
    public SegmentOverride segmentOverride = SegmentOverride.GROUPED;

    /**
     * 聚合方式。
     */
    public enum AggregationType implements DescriptorUseableShortComment {
        COUNT("计数（默认）"),
        SUM("求和"),
        AVERAGE("平均值"),
        MIN("最小值"),
        MAX("最大值"),
        APPROX_UNIQUE_COUNT("近似去重计数");

        private final String comment;

        AggregationType(String comment) {
            this.comment = comment;
        }

        @Override
        public String shortComment() {
            return comment;
        }
    }

    /**
     * 空值显示方式；仅折线图有效。
     */
    public enum NullDisplay implements DescriptorUseableShortComment {
        GAP("留空缺口（默认）"),
        IGNORED("忽略（连接前后有效值）"),
        ZEROES("视作 0");

        private final String comment;

        NullDisplay(String comment) {
            this.comment = comment;
        }

        @Override
        public String shortComment() {
            return comment;
        }
    }

    /**
     * 分段值的显示方式；仅柱状图有效。
     */
    public enum SegmentOverride implements DescriptorUseableShortComment {
        STACKED("堆叠"),
        PERCENTAGE("百分比"),
        GROUPED("分组并列");

        private final String comment;

        SegmentOverride(String comment) {
            this.comment = comment;
        }

        @Override
        public String shortComment() {
            return comment;
        }
    }

    @TISExtension
    public static class DefaultDescriptor extends Descriptor<SeriesConfig> {
        @Override
        public String getDisplayName() {
            return "Series Config";
        }
    }
}
