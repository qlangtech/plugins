package com.qlangtech.tis.plugin.ontology.workshop.model;

import com.google.common.collect.Lists;
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.IPluginStore;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.chartxy.AbstractSeriesAggregation;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.chartxy.CountAggregation;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

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
 * <h3>为什么没有 {@code variable}</h3>
 * 曾经有一个 {@code variable} 字段（{@code ordinal=1}，{@code SELECTABLE} + {@code require}），
 * 语义是「提供本系列数据的变量」。三重失效叠在一起，它在产品上<b>根本无法满足</b>：
 * <ul>
 *   <li><b>恒空候选</b>：{@code DefaultDescriptor} 给它注册的是
 *       {@code () -> Collections.emptyList()}，下拉里永远没有可选项；</li>
 *   <li><b>无法手输</b>：前端 SELECTABLE 走 {@code enum-icon-select}，该分支
 *       （{@code item-prop-val.component.ts} 的 {@code *ngSwitchCase="6"}）<b>不</b>绑定
 *       {@code allowCustomInput} —— 只有 ENUM 分支绑了它，所以「没有你要的选项？回车新增」的
 *       入口不会出现；</li>
 *   <li><b>没有读取方</b>：整个 {@code workshop} 包内除 {@code registerSelectOptions} 与
 *       {@code colsConfig()} 外无人读它，取数链路（{@code ChartXYDataProcessor}）只读
 *       {@code ChartLayer.dataSourceVar}。</li>
 * </ul>
 * 结果就是<b>添加系列的对话框永远保存不了</b>（服务端报
 * {@code prop:variable can not be empty}），ChartXY 无法完成一次配置。
 * 语义上也本就不该有它：「本系列的数据来自哪个变量」是图层的职责，一个图层只有一份数据源，
 * 层内各系列共用；让它逐系列重选，等于允许同一层内画来自不同变量的数据。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/15
 */
public class SeriesConfig implements Describable<SeriesConfig>, Serializable, IPluginStore.MultiDescribleElement {

    private static final long serialVersionUID = 1L;

    public static final String KEY_SEGMENT_BY = "segmentBy";

    /**
     * 聚合方式字段名。
     *
     * <p>抽成常量是为了让三级级联的 {@code toField} 能被测试逐字核对：
     * {@code "series[].aggregation.property"} 里的三层分别由前端 {@code propValChange}
     * 逐层下钻（列表字段 → 多态子表单 → 其子属性），写错一个字符就是静默失效。
     */
    public static final String KEY_AGGREGATION = "aggregation";

    /**
     * 系列名称（图例中显示的默认名称）。
     */
    @FormField(ordinal = 0, identity = true, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String name;

    // ordinal 1 留空：原 variable（「本系列的数据来自哪个变量」）已删除，见类注释「为什么没有
    // variable」。按本包既有约定，删字段后序位留空、不重排。

    /**
     * 聚合方式（多态）；默认计数。
     *
     * <p>度量属性 {@code property} 已上提到 {@link AbstractSeriesAggregation}：
     * 六种聚合里五种都需要它，只有计数不需要 —— 计数子类的 json 把那项关掉。
     */
    @FormField(ordinal = 2, validate = {Validator.require})
    public AbstractSeriesAggregation aggregation = new CountAggregation();

    // ordinal 3 留空：原 property 已下沉到 AbstractSeriesAggregation
    // （按本包既有约定，删字段后序位留空、不重排）。

    /**
     * 二次分段属性（可选）—— 按该属性把每个值再拆开。
     *
     * <p>候选列表由宿主 {@link ChartLayer} 的级联管道供给：
     * {@code valueChangePipe("dataSourceVar", "series[].segmentBy")}，即「数据源变量变了，
     * 逐行刷新分段属性候选」。本类自己<b>看不到</b>宿主的 {@code dataSourceVar}（元组行的
     * 校验上下文里没有兄弟字段的值），所以这里必须声明 {@code SELECTABLE} 而不是 {@code ENUM}：
     * SELECTABLE 的候选本来就是运行时供给的，ENUM 则意味着候选固定。
     * 先例见 {@code ObjectSetFilterConfig#property}。
     *
     * <p>取值是宿主对象类型的<b>属性名</b>，不是表列名 —— 聚合下推
     * （{@code AggregationPushdownPlanner}）会拿它做 {@code GROUP BY} 的第二个分组键，
     * 不存在的属性会在取数期报 {@code segmentBy column:'x' does not exist}。
     * 这也是把自由文本换成下拉的原因：一个拼错的属性名要到取数时才暴露。
     * <pre>
     *     SELECT xAxisProperty, segmentBy, AGG(property)   -- ← property 在这里
     *   FROM ...
     *   GROUP BY xAxisProperty, segmentBy                -- ↑ segmentBy 在这里
     * </pre>
     */
    @FormField(ordinal = 4, type = FormFieldType.SELECTABLE)
    public String segmentBy;

    /**
     * 图例显示名覆盖（可选）；分段图表中可覆盖单个分段的名称。
     */
    @FormField(ordinal = 5,type = FormFieldType.INPUTTEXT)
    public String displayOverride;

    /**
     * 系列配色（CSS 颜色值）。
     * use ButtonColor
     */
    @FormField(ordinal = 6,type = FormFieldType.INPUTTEXT)
    public String color;

    /**
     * 空值显示方式；仅折线图有效。
     */
    @FormField(ordinal = 7,type = FormFieldType.ENUM)
    public NullDisplay nullDisplay = NullDisplay.GAP;

    /**
     * 分段显示方式；仅柱状图有效。
     */
    @FormField(ordinal = 8,type = FormFieldType.ENUM)
    public SegmentOverride segmentOverride = SegmentOverride.GROUPED;

    @Override
    public String identityValue() {
        return this.name;
    }

    /**
     * XStream 反序列化不跑构造器与字段初值：聚合方式缺失时补成计数。
     */
    private Object readResolve() {
        if (this.aggregation == null) {
            this.aggregation = new CountAggregation();
        }
        return this;
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
    public static class DefaultDescriptor extends IPluginStore.MultiDescribleElementDescriptor<SeriesConfig> {

        public DefaultDescriptor() {
            super();
            // SELECTABLE 字段必须在 descriptor 上登记候选来源，否则 DescriptorsJSON#getSelectOptions
            // 直接抛异常、整张表单打不开。这里登记一个空列表作兜底：真实的候选由宿主
            // ChartLayer 的级联管道在渲染期回填（见 SeriesConfig#segmentBy）。
            // （原 variable 已删除，理由见 SeriesConfig 类注释）
            this.registerSelectOptions(KEY_SEGMENT_BY, Collections::emptyList);
        }

        @Override
        public String getDisplayName() {
            return "Series Config";
        }

        @Override
        public String shortComment() {
            return "图表数据系列";
        }

        @Override
        public ViewStyle viewStyle() {
            return ViewStyle.Table;
        }

        @Override
        public List<ColConfig> colsConfig() {
            // width 是百分比（见前端 MultiChildViewDescriptor.colWidth）
            return Lists.newArrayList(
                    new ColConfig("name", 25).setClickable(),
                    // 原 variable(30) 列已删除；把宽度让给 segmentBy，保持总宽 100
                    new ColConfig("segmentBy", 50),
                    new ColConfig("color", 25));
        }

        @Override
        public int maxHeight() {
            return 300;
        }
    }
}
