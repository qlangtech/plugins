package com.qlangtech.tis.plugin.ontology.workshop.model;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.WidgetOptionHelper;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 图表图层配置（Chart Layer）
 *
 * <h3>图层是什么</h3>
 * 一个图层 = <b>一份数据输入 + 一种画法</b>，多层叠加在同一个坐标系里构成「一张图表」。
 * 因此 Chart XY 并不是「一个 widget 一张图」，而是「一个 widget 若干图层」。
 *
 * <h3>为什么需要图层</h3>
 * 只有 {@code series} 平铺列表（本类引入前的形态）表达不了以下需求：
 * <ul>
 *   <li><b>多数据源叠图</b> —— 层 A 是「航班告警」对象集，层 B 是「机场」对象集</li>
 *   <li><b>组合图</b> —— 同一份数据的柱 + 线：层 A 柱状画 count，层 B 折线画 average，
 *       共用 X 轴但量纲不同（这也是「多值轴」配置项存在的前提）</li>
 *   <li><b>每层独立聚合与分段</b> —— 层 A 不分段，层 B 按「机型」分段</li>
 *   <li><b>每层独立输出变量</b> —— 各层的选择过滤产出各自的 ObjectSetFilter 变量</li>
 * </ul>
 * {@code series} 是<b>图层内部</b>按维度/度量做的次级区分；图层是「数据 → 图形」的映射单元。
 *
 * <h3>职责边界</h3>
 * <pre>
 *   Layer  ：数据从哪来、怎么聚合、画成什么形状、输出什么变量  ← 每层一份（本类）
 *   Axis   ：坐标轴长什么样（{@link AxisConfig}）              ← 每轴一份，与层数无关
 *   Legend ：由各层的 series 名与配色自动生成
 * </pre>
 * 因此「X 轴画哪个属性」属于图层（{@link #xAxisProperty}），
 * 「X 轴要不要显示标题」属于轴。旧实现把两者都塞进 AxisConfig，是职责错位。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/15
 */
public class ChartLayer implements Describable<ChartLayer>, Serializable {

    private static final long serialVersionUID = 1L;

    public static final String KEY_DATA_SOURCE_VAR = "dataSourceVar";

    /**
     * 图层标题 —— <b>仅供构建者</b>在多图层配置中组织与识别用，模块使用者看不到。
     * 官方文档原文："This title is not visible to module users, but is intended to help
     * builders organize and manage complex Chart XY configurations that use multiple Layers."
     */
    @FormField(ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String title;

    /**
     * 数据输入方式。
     *
     * <p>取值：对象集 / 函数聚合 / 时间序列集。其中<b>函数聚合</b>与<b>时间序列集</b>
     * 属于 P2 占位，当前尚未接线，选项与执行链路待补。
     */
    @FormField(ordinal = 1, type = FormFieldType.ENUM, validate = {Validator.require})
    public DataInputType dataInputType = DataInputType.OBJECT_SET;

    /**
     * 数据源变量名；选项由 {@code DescriptorImpl} 动态供给当前 Module 的对象集变量。
     */
    @FormField(ordinal = 2, type = FormFieldType.SELECTABLE, validate = {Validator.require})
    public String dataSourceVar;

    /**
     * 图层画法（柱 / 折线 / 散点）。
     *
     * <p>时间序列集输入仅支持折线。
     */
    @FormField(ordinal = 3, type = FormFieldType.ENUM, validate = {Validator.require})
    public LayerType layerType = LayerType.BAR;

    /**
     * 面积选项；仅 {@link LayerType#LINE} 有效。
     */
    @FormField(ordinal = 4, type = FormFieldType.ENUM, advance = true)
    public AreaOption areaOption = AreaOption.LINE;

    /**
     * 是否在图形上显示数值标签；柱状图与折线图支持。
     */
    @FormField(ordinal = 5, type = FormFieldType.ENUM, advance = true)
    public Boolean showLabels = false;

    /**
     * X 轴属性 —— 本图层按哪个属性分组（分组键）。
     *
     * <p>这是「轴绑定」的归属地：轴画什么由图层决定，轴长什么样由 {@link AxisConfig} 决定。
     * 当前为纯文本输入；待前端验证嵌套子表单的 {@code valueChangePipe} 级联后，
     * 可改为按 {@link #dataSourceVar} 联动的属性下拉。
     */
    @FormField(ordinal = 6, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String xAxisProperty;

    /**
     * 系列列表 —— 本图层内按维度/度量做的次级区分。
     */
    @FormField(ordinal = 7, type = FormFieldType.MULTI_SELECTABLE, validate = {Validator.require})
    public List<SeriesConfig> series = new ArrayList<>();

    /**
     * 将本图层的选择作为下游过滤条件（产出 ObjectSetFilter 变量）。
     *
     * <p>P2 占位：变量输出链路尚未接通，当前仅记录配置意图，不产生实际输出变量。
     */
    @FormField(ordinal = 8, type = FormFieldType.ENUM, advance = true)
    public Boolean selectionAsFilter = false;

    /**
     * 数据输入方式。
     */
    public enum DataInputType implements DescriptorUseableShortComment {
        OBJECT_SET("对象集变量（P0 已支持）"),
        FUNCTION_AGGREGATION("函数聚合（P2 占位）"),
        TIME_SERIES_SET("时间序列集（P2 占位）");

        private final String comment;

        DataInputType(String comment) {
            this.comment = comment;
        }

        @Override
        public String shortComment() {
            return comment;
        }
    }

    /**
     * 图层画法。
     */
    public enum LayerType implements DescriptorUseableShortComment {
        BAR("柱状图"),
        LINE("折线图"),
        SCATTER("散点图");

        private final String comment;

        LayerType(String comment) {
            this.comment = comment;
        }

        @Override
        public String shortComment() {
            return comment;
        }
    }

    /**
     * 面积选项；仅折线图有效。
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
    public static class DescriptorImpl extends Descriptor<ChartLayer> {

        public DescriptorImpl() {
            super();
            // 数据源变量选项：当前 Module 中的对象集变量
            this.registerSelectOptions(KEY_DATA_SOURCE_VAR, WidgetOptionHelper::getObjectSetVariableOptions);
        }

        @Override
        public String getDisplayName() {
            return "Chart Layer";
        }
    }
}
