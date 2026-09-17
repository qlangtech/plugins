package com.qlangtech.tis.plugin.ontology.workshop.model;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.IdentityName;
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
 *   Layer  ：数据从哪来、怎么聚合、画成什么形状、X 轴分组与分桶 ← 每层一份（本类）
 *   Axis   ：坐标轴长什么样（{@link AxisConfig}）              ← 每轴一份，与层数无关
 *   Legend ：由各层的 series 名与配色自动生成
 * </pre>
 * 其中「数据从哪来」由多态的 {@link LayerDataInput} 承担、「分组键怎么切」由多态的
 * {@link XAxisBucketing} 承担 —— 为什么是多态、为什么当前只有这几个子类、新增子类的门槛，
 * 均见各自类的 javadoc。
 * 因此「X 轴画哪个属性」（{@link #xAxisProperty}）与「X 轴怎么分桶」（{@link #xAxisBucketing}）
 * 都属于图层，「X 轴要不要显示标题」属于轴。旧实现把分组塞进 AxisConfig，是职责错位。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/15
 */
public class ChartLayer implements Describable<ChartLayer>, Serializable, IdentityName {

    private static final long serialVersionUID = 1L;

    /**
     * 图层标题 —— <b>仅供构建者</b>在多图层配置中组织与识别用，模块使用者看不到。
     * 官方文档原文："This title is not visible to module users, but is intended to help
     * builders organize and manage complex Chart XY configurations that use multiple Layers."
     */
    @FormField(identity = true, ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String title;

    /**
     * 数据输入来源 —— 本图层的数据从哪来。多态：具体输入方式自带自己的绑定字段与
     * 选项供给（见 {@link LayerDataInput}）。
     */
    @FormField(ordinal = 1, validate = {Validator.require})
    public LayerDataInput dataInput;

    /**
     * 图层画法（柱 / 折线 / 散点）。
     */
    @FormField(ordinal = 2, type = FormFieldType.ENUM, validate = {Validator.require})
    public LayerType layerType = LayerType.BAR;

    /**
     * 面积选项；仅 {@link LayerType#LINE} 有效。
     */
    @FormField(ordinal = 3, type = FormFieldType.ENUM, advance = true)
    public AreaOption areaOption = AreaOption.LINE;

    /**
     * 是否在图形上显示数值标签；柱状图与折线图支持。
     */
    @FormField(ordinal = 4, type = FormFieldType.ENUM, advance = true)
    public Boolean showLabels = false;

    /**
     * X 轴属性 —— 本图层按哪个属性分组（分组键）。
     *
     * <p>本字段只回答「按<b>谁</b>分组」；「这个分组键怎么<b>切</b>」由 {@link #xAxisBucketing}
     * 承担。两者分开是因为同一个属性切成「天」还是「小时」会得到两张完全不同的图，
     * 而分桶方式取决于属性的数据类型，不是本字段能表达的。
     *
     * <p>这是「轴绑定」的归属地：轴画什么由图层决定，轴长什么样由 {@link AxisConfig} 决定。
     * 当前为纯文本输入。注意：级联所需的 primary 字段必须是<b>同 descriptor 的普通
     * 非 Describable 字段</b>（见 {@code RootFormProperties.getInstancePropsJson}），
     * 而 {@link ObjectSetDataInput#dataSourceVar} 现已随多态拆分下沉到子表单 ——
     * 该级联在当前后端机制下不可达，故保持手输。前端若将来支持跨层 pipe 再接。
     */
    @FormField(ordinal = 5, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String xAxisProperty;

    /**
     * X 轴分桶方式 —— 分组键怎么切。
     *
     * <p>多态：时间戳/日期属性选 {@link TemporalBucketing}（粒度 + 粒度单位数），
     * 字符串/布尔属性选 {@link CategoricalBucketing}（取值方式）。为什么是多态、
     * 为什么 {@code limit} 在基类上，见 {@link XAxisBucketing} 的 javadoc。
     *
     * <p>本子表单独立于 {@link #dataInput}，<b>不</b>依赖 {@link #xAxisProperty} 的级联
     * —— 分桶方式由构建者按属性类型自选（同数据输入与轴性质的选择方式）。
     */
    @FormField(ordinal = 6, validate = {Validator.require})
    public XAxisBucketing xAxisBucketing;

    /**
     * 系列列表 —— 本图层内按维度/度量做的次级区分。
     */
    @FormField(ordinal = 7, type = FormFieldType.MULTI_SELECTABLE, validate = {Validator.require})
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
    @FormField(ordinal = 8, type = FormFieldType.SELECTABLE, advance = true)
    public String selectionAsFilterVar;

    @Override
    public String identityValue() {
        return this.title;
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
            // 候选值：当前 Module 中类型为「对象集合过滤器」的变量名
            // 先例：FilterListWidget.filterOutputVar 注册的是同一个供给方
            this.registerSelectOptions(KEY_SELECTION_AS_FILTER_VAR,
                    WidgetOptionHelper::getObjectSetFilterVariableOptions);
        }

        @Override
        public String getDisplayName() {
            return "Chart Layer";
        }
    }
}
