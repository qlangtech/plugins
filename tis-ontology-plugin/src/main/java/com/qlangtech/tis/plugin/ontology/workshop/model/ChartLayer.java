package com.qlangtech.tis.plugin.ontology.workshop.model;

import com.google.common.collect.Lists;
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.IPluginStore;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.impl.OntologyPluginMeta;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.WidgetOptionHelper;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.chartxy.AbstractLayerType;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.chartxy.AbstractSeriesAggregation;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.chartxy.BarLayerType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
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
 *   Layer  ：数据从哪来、怎么聚合、画成什么形状、X 轴分组与分桶 ← 每层一份（本类）
 *   Axis   ：坐标轴长什么样（{@link AxisConfig}）              ← 每轴一份，与层数无关
 *   Legend ：由各层的 series 名与配色自动生成
 * </pre>
 * 其中「数据从哪来」由 {@link #dataSourceVar} 指向的变量承担，「分组键怎么切」由多态的
 * {@link XAxisBucketing} 承担 —— 为什么是多态、为什么当前只有这两个子类、新增子类的门槛，
 * 见它的 javadoc。
 * 因此「X 轴画哪个属性」（{@link #xAxisProperty}）与「X 轴怎么分桶」（{@link #xAxisBucketing}）
 * 都属于图层，「X 轴要不要显示标题」属于轴。旧实现把分组塞进 AxisConfig，是职责错位。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/15
 */
public class ChartLayer implements Describable<ChartLayer>, Serializable, IPluginStore.MultiDescribleElement {

    private static final long serialVersionUID = 1L;

    /**
     * 图层标题 —— <b>仅供构建者</b>在多图层配置中组织与识别用，模块使用者看不到。
     * 官方文档原文："This title is not visible to module users, but is intended to help
     * builders organize and manage complex Chart XY configurations that use multiple Layers."
     */
    @FormField(identity = true, ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String title;

    public static final String KEY_DATA_SOURCE_VAR = "dataSourceVar";
    public static final String KEY_X_AXIS_PROPERTY = "xAxisProperty";
    /**
     * 图层画法（柱 / 折线 / 散点），多态。
     *
     * <p>各画法专有的配置（面积选项、数值标签）落在对应子类里，因此散点图上不会
     * 出现「面积选项」这种与自己无关的字段。见 {@link AbstractLayerType}。
     */
    @FormField(ordinal = 1, validate = {Validator.require})
    public AbstractLayerType layerType = new BarLayerType();
    /**
     * 数据输入来源 —— 本图层的数据从哪个<b>对象集合变量</b>来。
     *
     * <h3>为什么是扁平字段而不是多态子表单</h3>
     * 早先这里是 {@code LayerDataInput dataInput}（多态，唯一子类 {@code ObjectSetDataInput}
     * 只持有本字段）。扁平化的直接动因是级联：TIS 的 {@code ValueChangePipe} 要求 primary
     * 必须是<b>同 descriptor 的普通非 Describable 字段</b>
     * （见 {@code RootFormProperties.getInstancePropsJson}），字段埋在子表单里时
     * {@link #xAxisProperty} 的候选属性就永远取不到。字段集既未真正分叉，这层多态只是
     * 一层包壳 —— 判据与原 {@code LayerDataInput} 的 javadoc 一致。
     *
     * <h3>为什么选项供给用 getObjectSetSourceVariableOptions</h3>
     * OBJECT_SET 类型的变量有两条来源：{@code ObjectSetDefinitionConfig}（可执行查询）与
     * {@code ExternalInputConfig}（行点击等交互写入的选中载体，没有查询语义）。后者选中后
     * 会在 {@code ObjectSetVariable.resolveDataSourceBinding} 抛 illegal type，故这里只列前者
     * ——先例 {@code ObjectTableWidget.objectSetVar} 用的是同一个供给方。
     *
     * <p>将来若要支持时间序列集合作为图层数据源，把选项放宽成
     * {@code getVariableOptions(OBJECT_SET, TIME_SERIES_SET)}（先例 {@code TimelineWidget}），
     * 并在取数侧按变量类型分流即可，<b>不需要</b>再新建多态子类。
     */
    @FormField(ordinal = 2, type = FormFieldType.SELECTABLE, validate = {Validator.require})
    public String dataSourceVar;

    // ordinal 3 / 4 留空：原 areaOption / showLabels 已分别下沉到折线 / 柱线画法子类
    // （按本包既有约定，删字段后序位留空、不重排）。



    /**
     * X 轴属性 —— 本图层按哪个属性分组（分组键）。
     *
     * <p>本字段只回答「按<b>谁</b>分组」；「这个分组键怎么<b>切</b>」由 {@link #xAxisBucketing}
     * 承担。两者分开是因为同一个属性切成「天」还是「小时」会得到两张完全不同的图，
     * 而分桶方式取决于属性的数据类型，不是本字段能表达的。
     *
     * <p>这是「轴绑定」的归属地：轴画什么由图层决定，轴长什么样由 {@link AxisConfig} 决定。
     * 候选值随 {@link #dataSourceVar} 级联（见 {@code DescriptorImpl} 的 {@code valueChangePipe}）：
     * 选中数据源变量后，下拉里是该对象类型的属性名。级联要求 primary 是同层普通字段，
     * 这正是 {@link #dataSourceVar} 保持扁平而非下沉子表单的原因。
     */
    @FormField(ordinal = 5, type = FormFieldType.SELECTABLE, validate = {Validator.require})
    public String xAxisProperty;

    /**
     * X 轴分桶方式 —— 分组键怎么切。
     *
     * <p>多态：时间戳/日期属性选 {@link TemporalBucketing}（粒度 + 粒度单位数），
     * 字符串/布尔属性选 {@link CategoricalBucketing}（取值方式）。为什么是多态、
     * 为什么 {@code limit} 在基类上，见 {@link XAxisBucketing} 的 javadoc。
     *
     * <p>本子表单独立于 {@link #dataSourceVar}，<b>不</b>依赖 {@link #xAxisProperty} 的级联
     * —— 分桶方式由构建者按属性类型自选（同数据源与轴性质的选择方式）。
     */
    @FormField(ordinal = 6, validate = {Validator.require})
    public XAxisBucketing xAxisBucketing;

    /**
     * 系列列表字段名。
     *
     * <p>抽成常量是为了让级联声明的 {@code toField} 可以被测试逐字核对：
     * {@code "series[]." + SeriesConfig.KEY_SEGMENT_BY} 里的 {@code []} 是前端
     * {@code propValChange} 的语法标记（「作用于列表里的每一条记录」），后端只按原样字符串
     * 存与回传，写错一个字符就是静默失效。
     */
    public static final String KEY_SERIES = "series";

    /**
     * 系列列表 —— 本图层内按维度/度量做的次级区分。
     *
     * <p>行内的 {@link SeriesConfig#segmentBy} 候选随 {@link #dataSourceVar} 级联
     * （见 {@code DescriptorImpl} 的 {@code valueChangePipe}），故两个字段必须<b>同层</b>：
     * 级联的 primary 只能引用同 descriptor 里的普通字段。
     */
    @FormField(ordinal = 7, desClazz = SeriesConfig.class, type = FormFieldType.MULTI_DESCRIBLE_PLUGIN, validate = {Validator.require})
    public List<SeriesConfig> series = new ArrayList<>();

    public static final String KEY_SELECTION_AS_FILTER_VAR = "selectionAsFilterVar";

    /**
     * 本图层的选择写回哪个「对象集合过滤器」变量。
     *
     * <p>即 Palantir 图层的 <b>Selection as filter</b>：「允许对本图层的选择做下游组件过滤，
     * 经由输出的 <i>Object set filter</i> 变量」。该变量是模块里<b>已存在</b>的、类型为
     * {@link com.qlangtech.tis.plugin.ontology.workshop.enums.VariableType#OBJECT_SET_FILTER}
     * 的变量 —— 绑定语义是「本图层往哪个变量里写」，变量本身在变量面板里先行创建。
     *
     * <p>候选值由 {@code DescriptorImpl} 动态供给当前 Module 的对象集合过滤器变量，形态对齐
     * {@code FilterListWidget.filterOutputVar}（同为「写回一个对象集合过滤器变量」）。
     *
     * <p><b>留空是合法状态</b>，表示不启用该功能，故不挂 {@code Validator.require}
     * —— Palantir 该选项本身就是 Optional 的。
     *
     * <p>字段名不沿用旧的 {@code selectionAsFilter}：类型已由布尔变成变量名，旧名读起来
     * 仍像开关；且 {@code *Var} 后缀是本包变量绑定字段的命名约定（{@code dataSourceVar}、
     * {@code filterOutputVar}、{@code activeObjectVar} …）。
     */
    @FormField(ordinal = 8,type = FormFieldType.SELECTABLE)
    public String selectionAsFilterVar;

    @Override
    public String identityValue() {
        return this.title;
    }

    /**
     * XStream 反序列化不跑构造器与字段初值：历史落盘的图层可能整个缺失
     * {@code layerType} 或 {@code series}，前者会让表单的 impl 选择器拿到 null，
     * 后者会让取数直接 NPE。
     */
    private Object readResolve() {
        if (this.layerType == null) {
            this.layerType = new BarLayerType();
        }
        if (this.series == null) {
            this.series = new ArrayList<>();
        }
        return this;
    }

    @TISExtension
    public static class DescriptorImpl extends IPluginStore.MultiDescribleElementDescriptor<ChartLayer> {

        public DescriptorImpl() {
            super();
            // 候选值：当前 Module 中「对象集合定义」型的变量名（ExternalInput 型没有查询语义）
            // 先例：ObjectTableWidget.objectSetVar 注册的是同一个供给方
            this.registerSelectOptions(KEY_DATA_SOURCE_VAR,
                    WidgetOptionHelper::getObjectSetSourceVariableOptions);

            // 级联属性字段：初始为空（未选 dataSourceVar 时无候选）。
            // 必须注册：SELECTABLE 字段未注册会让 DescriptorsJSON.getSelectOptions 直接抛异常，
            // 整张表单打不开。先例见 ChartPieWidget.labelProperty。
            this.registerSelectOptions(KEY_X_AXIS_PROPERTY, Collections::emptyList);

            // 同一个 fromField 驱动三个目标字段：
            //   ① xAxisProperty               —— 本图层的分组键（同层普通字段）
            //   ② series[].segmentBy          —— 每一行的二次分段属性（'[]' 表示逐行回填，
            //                                    解析点只在前端 item-prop-val.component.ts#propValChange）
            //   ③ series[].aggregation.property —— 每一行的度量属性，比 ② 再深一层：
            //                                    穿越多态子表单（aggregation 有 6 个 impl），
            //                                    前端会逐个改写每个 impl 的 AttrDesc 候选
            // 三者的候选完全相同（都是 dataSourceVar 所指对象类型的属性名），故共用一次广播
            // render（ValueChangePipe 对广播只调用一次，多个 toField 复用同一结果）。
            // 若将来某目标候选形态不同，再改用 render(toField, fn) 逐字段覆盖。
            this.valueChangePipe(KEY_DATA_SOURCE_VAR, KEY_X_AXIS_PROPERTY, //
                            KEY_SERIES + "[]." + SeriesConfig.KEY_SEGMENT_BY, //
                            KEY_SERIES + "[]." + SeriesConfig.KEY_AGGREGATION + "." + AbstractSeriesAggregation.KEY_PROPERTY)
                    .render((pluginMeta, params) -> {
                        String selectedVar = params.getString(KEY_DATA_SOURCE_VAR);
                        if (selectedVar == null) {
                            return Collections.emptyList();
                        }
                        return WidgetOptionHelper.getObjectPropertyOptions(
                                selectedVar, OntologyPluginMeta.createPluginMeta(pluginMeta));
                    });

            // 候选值：当前 Module 中类型为「对象集合过滤器」的变量名
            // 先例：FilterListWidget.filterOutputVar 注册的是同一个供给方
            this.registerSelectOptions(KEY_SELECTION_AS_FILTER_VAR,
                    WidgetOptionHelper::getObjectSetFilterVariableOptions);
        }

        @Override
        public String getDisplayName() {
            return "Chart Layer";
        }

        @Override
        public String shortComment() {
            return "图表图层";
        }

        @Override
        public ViewStyle viewStyle() {
            return ViewStyle.Table;
        }

        @Override
        public List<ColConfig> colsConfig() {
            // width 是百分比（见前端 MultiChildViewDescriptor.colWidth）
            return Lists.newArrayList(
                    new ColConfig("title", 30).setClickable(),
                    new ColConfig("xAxisProperty", 40),
                    new ColConfig("selectionAsFilterVar", 30));
        }

        @Override
        public int maxHeight() {
            return 300;
        }
    }
}
