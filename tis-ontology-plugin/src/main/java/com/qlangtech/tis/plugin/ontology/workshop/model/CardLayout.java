package com.qlangtech.tis.plugin.ontology.workshop.model;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;

/**
 * 卡片布局：每个指标一张卡片。可配排列方向、标签/数值模板与时间序列可视化。
 *
 * <p>时间序列可视化（{@link #visualization}）只挂在卡片布局上：标签 / 列表布局没有
 * 「每个指标一张卡」的形态，承载该配置只会得到一个永不渲染的字段。
 *
 * @author 百岁 (baisui@qlangtech.com)
 */
public class CardLayout extends LayoutStyle {

    private static final long serialVersionUID = 1L;

    @FormField(ordinal = 0, type = FormFieldType.ENUM)
    public Direction direction = Direction.HORIZONTAL;

    @FormField(ordinal = 1, type = FormFieldType.ENUM)
    public Template template = Template.STACKED;

    /**
     * 时间序列可视化（off / 启用两态多态，见 {@link AbstractTimeSeriesVisualization}）。
     */
    @FormField(ordinal = 2, validate = {Validator.require})
    public AbstractTimeSeriesVisualization visualization = new TimeSeriesVisualizationOff();

    /**
     * XStream 反序列化不跑构造器与字段初值：早于本次改造落盘的卡片布局里没有该元素，
     * 补成 off，避免表单渲染时 NPE。
     */
    private Object readResolve() {
        if (this.visualization == null) {
            this.visualization = new TimeSeriesVisualizationOff();
        }
        return this;
    }

    @TISExtension
    public static class DefaultDescriptor extends BasicDescriptor implements DescriptorUseableShortComment {

        @Override
        public String getDisplayName() {
            return "Card Layout";
        }

        @Override
        public String shortComment() {
            return "卡片：每个指标一张卡片";
        }
    }
}
