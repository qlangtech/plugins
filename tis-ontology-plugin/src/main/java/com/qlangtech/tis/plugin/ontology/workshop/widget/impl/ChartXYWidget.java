package com.qlangtech.tis.plugin.ontology.workshop.widget.impl;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.SubForm;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.workshop.model.AxisConfig;
import com.qlangtech.tis.plugin.ontology.workshop.model.ChartLayer;
import com.qlangtech.tis.plugin.ontology.workshop.widget.WorkshopWidget;
import com.qlangtech.tis.plugin.workshop.widget.IWorkshopWidget;

import java.util.ArrayList;
import java.util.List;

/**
 * P0 Widget：XY 图表（Chart XY）
 *
 * <p>柱状图 / 折线图 / 散点图。一个 widget 由若干<b>图层</b>（{@link ChartLayer}）叠加而成，
 * 每个图层自带一份数据输入与一种画法；坐标轴的外观与图层数无关，由
 * {@link #xAxis} / {@link #yAxis} 统一描述。
 *
 * <h3>三层配置的职责边界</h3>
 * <pre>
 *   Layer  ：数据从哪来、怎么聚合、画成什么形状、X 轴按什么分组   ← 每层一份
 *   Axis   ：坐标轴长什么样（标题 / 刻度 / 网格线 / 数值格式化）  ← 每轴一份
 *   Legend ：由各层的 series 名与配色自动生成，此处只配显不显示、放哪
 * </pre>
 *
 * <h3>演进说明</h3>
 * 坐标轴与系列字段原先是 ontology 侧的 {@code ChartXYSetupConfig}（{@code WidgetSetupConfig}
 * 的子类，零引用），随 {@code WidgetSetupConfig} 一并删除后并入本类；本轮进一步把
 * 扁平的 {@code series} 列表换成 {@code layers}，并把 {@link AxisConfig} 由「一个类装
 * 三种刻度」改为按轴性质多态的子类体系。详见
 * {@code detail-design/workshop/05-08-widget-implementation-chartXY-widget.md}。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/12
 */
public class ChartXYWidget extends WorkshopWidget {

    /**
     * 图层列表 —— 一个图层 = 一份数据输入 + 一种画法，多层叠在同一坐标系。
     */
    // @FormField(ordinal = 10, type = FormFieldType.MULTI_SELECTABLE, validate = {Validator.require})
    @SubForm(ordinal = 10, desClazz = ChartLayer.class //
            , idListGetScript = "return com.qlangtech.tis.coredefine.module.action.DataxAction.getTablesInDB(filter);", atLeastOne = true)
    public List<ChartLayer> layers = new ArrayList<>();

    /**
     * X 轴（水平轴）配置。多态：按需选分类轴或连续轴。
     */
    @FormField(ordinal = 11, validate = {Validator.require})
    public AxisConfig xAxis;

    /**
     * Y 轴（垂直轴）配置。多态：按需选分类轴或连续轴。
     */
    @FormField(ordinal = 12, validate = {Validator.require})
    public AxisConfig yAxis;

    /**
     * 是否显示图例。
     */
    @FormField(ordinal = 13, type = FormFieldType.ENUM, advance = true)
    public Boolean showLegend = true;

    /**
     * 图例位置；仅 {@link #showLegend} 开启时生效。
     */
    @FormField(ordinal = 14, type = FormFieldType.ENUM, advance = true)
    public LegendPosition legendPosition = LegendPosition.BOTTOM;

    /**
     * 图表方向。柱状图默认横向，折线图与散点图仅支持纵向。
     */
    @FormField(ordinal = 15, type = FormFieldType.ENUM, advance = true)
    public Orientation orientation = Orientation.VERTICAL;

    /**
     * 图例位置。
     */
    public enum LegendPosition implements DescriptorUseableShortComment {
        TOP("上方"),
        BOTTOM("下方"),
        LEFT("左侧"),
        RIGHT("右侧");

        private final String comment;

        LegendPosition(String comment) {
            this.comment = comment;
        }

        @Override
        public String shortComment() {
            return comment;
        }
    }

    /**
     * 图表方向。
     */
    public enum Orientation implements DescriptorUseableShortComment {
        HORIZONTAL("横向（柱状图默认）"),
        VERTICAL("纵向（折线/散点仅此）");

        private final String comment;

        Orientation(String comment) {
            this.comment = comment;
        }

        @Override
        public String shortComment() {
            return comment;
        }
    }

    @TISExtension
    public static class DescriptorImpl extends BaseWidgetDescriptor<IWorkshopWidget> {

        @Override
        public String getDisplayName() {
            return "Chart XY";
        }

        @Override
        public WorkShopWidgetType getWidgetType() {
            return WorkShopWidgetType.CHART_XY;
        }
    }
}
