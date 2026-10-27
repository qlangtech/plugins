package com.qlangtech.tis.plugin.ontology.workshop.widget.impl;

import com.alibaba.citrus.turbine.Context;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.workshop.model.AxisConfig;
import com.qlangtech.tis.plugin.ontology.workshop.model.CategoricalAxisConfig;
import com.qlangtech.tis.plugin.ontology.workshop.model.ChartLayer;
import com.qlangtech.tis.plugin.ontology.workshop.model.ContinuousAxisConfig;
import com.qlangtech.tis.plugin.ontology.workshop.model.XAxisBucketing;
import com.qlangtech.tis.plugin.ontology.workshop.widget.FullDisplayWidget;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.chartxy.AbstractLayerType;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.chartxy.AbstractLegendConfig;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.chartxy.ChartXYDataProcessor;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.chartxy.LegendConfig;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.chartxy.LineLayerType;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.chartxy.ScatterLayerType;
import com.qlangtech.tis.plugin.workshop.widget.IWorkshopWidget;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.util.AttrValMap;
import com.qlangtech.tis.util.IPluginContext;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

import static com.qlangtech.tis.plugin.annotation.FormFieldType.MULTI_DESCRIBLE_PLUGIN;

/**
 * P0 Widget：XY 图表（Chart XY）
 *
 * <p>柱状图 / 折线图 / 散点图。一个 widget 由若干<b>图层</b>（{@link ChartLayer}）叠加而成，
 * 每个图层自带一份数据输入与一种画法；坐标轴的外观与图层数无关，由
 * {@link #xAxis} / {@link #yAxis} 统一描述。
 *
 * <h3>配置的职责边界（Layer / Axis / Legend / Orientation）</h3>
 * <pre>
 *   Layer  ：数据从哪来、怎么聚合、画成什么形状、X 轴按什么分组   ← 每层一份
 *   Axis   ：坐标轴长什么样（标题 / 刻度 / 网格线 / 数值格式化）  ← 每轴一份
 *   Legend ：由各层的 series 名与配色自动生成，此处只配显不显示、放哪
 *   Orient ：整张图的坐标系方向（索引轴放哪条边）                 ← 全图一份
 * </pre>
 * 其中 {@link #orientation} <b>不是图层属性</b>：所有图层共享同一对坐标轴，
 * 因此「哪些图层组合允许横向」是整图级约束，规则与原因见该字段注释。
 *
 * <h3>演进说明</h3>
 * 坐标轴与系列字段原先是 ontology 侧的 {@code ChartXYSetupConfig}（{@code WidgetSetupConfig}
 * 的子类，零引用），随 {@code WidgetSetupConfig} 一并删除后并入本类；本轮进一步把
 * 扁平的 {@code series} 列表换成 {@code layers}，并把 {@link AxisConfig} 由「一个类装
 * 三种刻度」改为按轴性质多态的子类体系。详见
 * {@code /opt/misc/palantir-study/use-case-data-quality/detail-design/workshop/05-08-widget-implementation-chartXY-widget.md}。
 * @code /opt/misc/palantir-study/foundry/workshop/detail/04-widgets/widgets-chart.md
 * @code /Users/mozhenghua/j2ee_solution/project/tis-console/src/workshop/components/widgets/chart-xy/chart-xy.component.ts
 *
 * https://www.chartjs.org/docs/latest/samples/line/line.html
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/12
 */
public class ChartXYWidget extends FullDisplayWidget {

    /**
     * 图层列表 —— 一个图层 = 一份数据输入 + 一种画法，多层叠在同一坐标系。
     */
    // @FormField(ordinal = 10, type = FormFieldType.MULTI_SELECTABLE, validate = {Validator.require})
    @FormField(ordinal = 10, desClazz = ChartLayer.class, type = MULTI_DESCRIBLE_PLUGIN, validate = {Validator.require})
    public List<ChartLayer> layers = new ArrayList<>();

    /**
     * 逻辑 X 轴（分桶轴）配置 —— <b>当前只能选 {@link CategoricalAxisConfig}</b>。
     *
     * <p>X 上的取值不是用户直接指定的属性，而是各图层按 {@code ChartLayer.xAxisProperty}
     * 分组后得到的<b>离散标签</b>（分类键或时间桶，见 {@code ChartXYDataProcessor#resolveXKey}）。
     * 因此哪怕轴属性是 timestamp，X 轴也是分类轴 —— 时间不是一种刻度类型，见
     * {@link ContinuousAxisConfig} 的类注释。
     *
     * <p>类型声明为多态是为将来放开「连续 X 轴」（真散点图：X、Y 都是数值）留位；
     * 在那条数据链路落地之前，连续 X 轴会被 {@link #axisKindViolation()} 拒绝 ——
     * 配上去不会报错，只会因为标签落进数值刻度而画不出线。
     *
     * <p><b>字段名指逻辑轴而非画面位置</b>：{@code orientation=HORIZONTAL} 时本配置被前端
     * 渲染到屏幕上垂直的那条边（见 {@code chart-xy.component.ts} 的 {@code buildOptions}），
     * 但「分桶轴」这个身份不变 —— 所以本约束与图表方向无关。
     *
     * <p><b>表单初值写在 json 里</b>：{@code ChartXYWidget.json} 的
     * {@code xAxis.dftVal = "Categorical Axis"}。前端新建时按 {@code dftVal} 与各 descriptor
     * 的 {@code displayName} 逐字匹配来预选 impl（{@code tis.plugin.ts} 的
     * {@code addNewEmptyItemProp}），Java 字段初值不参与这条通路 —— 在这里写
     * {@code new CategoricalAxisConfig()} 只会影响代码里 new 出来的实例。
     */
    @FormField(ordinal = 11, validate = {Validator.require})
    public AxisConfig xAxis;

    /**
     * 逻辑 Y 轴（值轴）配置 —— <b>当前只能选 {@link ContinuousAxisConfig}</b>。
     *
     * <p>Y 上画的是各系列的聚合结果（count / sum / avg 等数值），分类轴在此无意义。
     * 约束与原因同 {@link #xAxis}；表单初值同样走 json 的
     * {@code yAxis.dftVal = "Continuous Axis"}，不是 Java 字段初值。
     */
    @FormField(ordinal = 12, validate = {Validator.require})
    public AxisConfig yAxis;

    /**
     * 图例配置（off / 显示两态多态）。
     *
     * <p>「显示」子类自带 {@code legendPosition} —— 位置不再是顶层平铺字段，
     * 因此「关了图例却还留着位置」这种状态在配置层面就不存在了。
     */
    @FormField(ordinal = 13, validate = {Validator.require})
    public AbstractLegendConfig showLegend = new LegendConfig();

    // ordinal 14 留空：原顶层 legendPosition 已下沉到 LegendConfig
    // （按本包既有约定，删字段后序位留空、不重排）。

    /**
     * 图表方向 —— <b>整张图共用的一套坐标系设置，不是图层属性</b>。
     *
     * <h4>硬性约束（保存期由 {@link DescriptorImpl#verify} 强制，运行期由 {@link #orientationViolation()} 兜底）</h4>
     * <pre>
     *   HORIZONTAL 合法 ⟺ layers 非空 且 每一层的 layerType 都支持横向（当前只有 BarLayerType）
     * </pre>
     * 只要图层中出现折线或散点，整张图只能取 {@link Orientation#VERTICAL}。
     * 这条规则与图层数量无关：单个折线/散点图层同样不能横向；多个图层全是柱状时横向有效
     * ——所有柱子一起旋转，不存在「某层横、某层竖」（方向只有一个取值）。
     *
     * <h4>为什么必须是图级、且只能是这个规则（改之前请读完）</h4>
     * <ol>
     *   <li><b>一张图只有一对 scale</b>：{@link #xAxis} / {@link #yAxis} 各一个，所有图层共享。
     *       横向柱要求 y=类目轴（桶标签）、x=数值轴（聚合值）；纵向折线/散点要求 x=类目轴、
     *       y=数值轴。同一对轴不可能同时满足这两种要求 —— 这是「混合即坏」的根因，
     *       不是前端渲染 bug。</li>
     *   <li><b>Chart.js 的 {@code indexAxis} 是 dataset 选项，但引用的仍是共享 scale</b>。
     *       要让每层各转各的，必须给 dataset 配 {@code xAxisID}/{@code yAxisID} 并额外造一套
     *       scale，那等于把两个坐标系叠在同一块画布上：图层不再共享类目轴、同一个桶在画面上
     *       对不齐，「多层叠在同一坐标系」的设计前提（见类注释）随之失效，且
     *       {@code AxisConfig} 的「x 一套 / y 一套」模型也表达不了。</li>
     *   <li><b>折线/散点控制器不认 {@code indexAxis}</b>：它们的数据始终按「labels → x 轴、
     *       聚合值 → y 轴」解析。前端在横向时只把两根轴的<b>配置</b>整对换位、并不换数据
     *       （见 {@code chart-xy.component.ts} 的 {@code buildOptions}），类目字符串会落进
     *       数值轴 —— 线画不出来或错位。因此「仅纵向」是唯一能正常渲染的状态，不是风格建议。</li>
     *   <li><b>不要把本字段下沉到 {@link com.qlangtech.tis.plugin.ontology.workshop.widget.impl.chartxy.BarLayerType}</b>：
     *       那只是消除了「折线/散点身上挂着一个它不支持的字段」这一语法问题；两个柱图层
     *       仍可一个横一个竖，对 scale 的要求同样互斥。而「兄弟字段必须取值一致」类型系统
     *       表达不了，最终仍要在图级补一条跨字段校验 —— 只是把同一个不变量拆散成 N 个字段。</li>
     * </ol>
     *
     * <p>默认取 {@link Orientation#VERTICAL}（Palantir 中纯柱图默认横向）。不跟随该默认是因为
     * 图层列表可以在保存方向之后继续增删：HORIZONTAL 初值会让「新增一个折线图层」立刻
     * 变成非法配置。
     *
     * <p><b>默认值有两处，缺一不可</b>：
     * <ol>
     *   <li>表单初值 —— {@code ChartXYWidget.json} 的 {@code orientation.dftVal}，取枚举
     *       <b>常量名</b> {@code "VERTICAL"}（不是 {@code shortComment}），前端新建时据此
     *       预选选项。</li>
     *   <li>下面这个 Java 字段初值 —— 构造函数执行，因此对代码里 {@code new} 出来的实例、
     *       以及保存时表单缺项的情况（{@code parseDescribable} 走 {@code clazz.newInstance()}）
     *       生效。<b>XStream 反序列化不走构造器也不执行字段初值</b>，历史数据由
     *       {@link #readResolve()} 单独回填同一个值。</li>
     * </ol>
     * 反过来说：只写 Java 初值，用户在表单里看到的仍是空选项，必填项要手工选一次才能保存；
     * 只写 json，代码里 new 出来再直接落盘的实例（测试、迁移脚本）就是 null。
     * 两处取同一个值由 {@code TestChartXYStructure#requiredPolymorphicFieldsDeclareDftVal}
     * 保证。
     */
    @FormField(ordinal = 15, type = FormFieldType.ENUM)
    public Orientation orientation = Orientation.VERTICAL;

    /**
     * 图表方向。
     *
     * <p>{@code shortComment} 会成为表单里的选项说明，因此把可用条件直接写在这里，
     * 让构建者在选择时就能看到约束（而不是保存时才报错）。完整规则见
     * {@link ChartXYWidget#orientation}。
     */
    public enum Orientation implements DescriptorUseableShortComment {
        HORIZONTAL("横向（仅全部图层为柱状图时可用）"),
        VERTICAL("纵向（含折线/散点图层时唯一可选）");

        private final String comment;

        Orientation(String comment) {
            this.comment = comment;
        }

        @Override
        public String shortComment() {
            return comment;
        }
    }

    /**
     * XStream 反序列化不走构造器与字段初值：空值回填 + 轴的嵌套配置归一化。
     */
    private Object readResolve() {
        if (this.showLegend == null) {
            this.showLegend = new LegendConfig();
        }
        if (this.orientation == null) {
            // 字段上线前落盘的 widget 没有该元素：按初值纵向回填（也是唯一永远合法的一侧）
            this.orientation = Orientation.VERTICAL;
        }
        if (this.xAxis != null) {
            this.xAxis.normalize();
        }
        if (this.yAxis != null) {
            this.yAxis.normalize();
        }
        return this;
    }

    /**
     * 校验 {@link #orientation} 与图层画法是否相容 —— 规则与原因的权威说明见
     * {@link #orientation} 字段注释。
     *
     * <p>调用点：保存期 {@link DescriptorImpl#verify}（字段级报错、中止写盘）与运行期
     * {@link ChartXYDataProcessor#process}（兜住历史落盘数据或绕过表单的直接调用）。
     *
     * @return {@code null} 表示合法；否则为可直接展示给用户的错误描述
     */
    public String orientationViolation() {
        if (this.orientation != Orientation.HORIZONTAL || this.layers == null) {
            return null;
        }
        for (int i = 0; i < this.layers.size(); i++) {
            ChartLayer layer = this.layers.get(i);
            // layerType 为 null 是历史数据，取数侧按柱状兜底（见 ChartXYDataProcessor），这里同样放行
            if (layer == null || layer.layerType == null || layer.layerType.supportsHorizontal()) {
                continue;
            }
            String layerName = StringUtils.isBlank(layer.title) ? ("#" + (i + 1)) : "「" + layer.title + "」";
            return "图表方向不能设为横向：图层" + layerName + " 是" + layerTypeLabel(layer.layerType)
                    + "，只有柱状图图层支持横向。请把图表方向改为纵向，或调整该图层的画法。";
        }
        return null;
    }

    /**
     * 校验 {@link #xAxis} / {@link #yAxis} 的<b>轴性质</b>（分类 / 连续）是否与当前支持的
     * 数据链路相符。
     *
     * <h4>当前规则（按轴位断言，而不是「不许两边相同」）</h4>
     * <pre>
     *   xAxis instanceof CategoricalAxisConfig  且  yAxis instanceof ContinuousAxisConfig
     * </pre>
     * 为什么是「按轴位」而不是「一分类一连续即可」：{@code x=连续 / y=分类} 这种配对<b>同样</b>
     * 会被当前渲染器画崩（分桶标签落进线性刻度 → 空图），「不许两边相同」拦不住它。
     *
     * <h4>为什么现在必须这样</h4>
     * X 取值来自服务端分桶（{@code ChartXYDataProcessor#assemble} 输出的点 {@code x} 恒为
     * 字符串标签），Y 取值来自聚合函数（恒为数值）。这条规则<b>不是</b>「暂时把散点图拦在外面」
     * —— 散点图本来就是「分类 X × 聚合值 Y」，与柱/线共用同一套 X 轴语义。
     *
     * <h4>证据：Palantir 的散点图也是分类 X 轴（勿凭直觉推翻）</h4>
     * 直觉上「散点图 = X、Y 都是数值」，但它不是。官方文档的示例图
     * {@code /opt/misc/palantir-study/foundry/workshop/detail/resources/chart_xy_example.png}
     * 第三张面板「# of Alerts by Time of Delay」的 X 轴刻度可度量：标签为
     * {@code 0.0 1.0 2.0 3.0 4.0 5.0 7.0 8.0 10.0}，对应的刻度线像素间距为
     * 31.0 / 32.0 / 31.5 / 31.5 / 31.5 / <b>31.5</b> / 32.0 / <b>31.0</b> ——
     * <b>恒定在 31–32px</b>，而 {@code 5.0→7.0}、{@code 8.0→10.0} 的数值跨度是 2。
     * 连续轴上这两段应宽一倍（约 63px），实测没有 —— 说明每个<b>出现过的取值</b>都占一格、
     * 均匀铺开，只是恰好没有 6 与 9 这个值的数据，故标签里缺这两项。
     *
     * <p>这与文档对图层配置的描述自洽：每层仍有 {@code X axis property}（「Determines the
     * property type plotted」）、{@code Series aggregation}（默认 Count）与 {@code Segment by}
     * —— 这些只有「按属性分桶」才讲得通。
     *
     * <p>因此「连续 X 轴」是一条<b>尚未实现的链路</b>（{@link XAxisBucketing} 只有时间/分类
     * 两个子类、{@code AggregationPushdownPlanner} 只接受这两种分桶、前端把散点降级成
     * {@code line + showLine:false} 挂在类目轴上），放开只会让用户配出一个不报错但画不出点的图
     * —— 正是本类 {@link #orientation} 那段注释要防的静默失效。
     *
     * <h4>如果将来要做「数值对散点」：升级成图级 chart mode，<b>不是</b>删掉本条检查</h4>
     * 注意这里的措辞：数值对散点（真正的二维数值相关图）是<b>超出 Palantir 能力的扩展</b>，
     * 不是「补上 Palantir 已经有的功能」。而
     * {@link CategoricalAxisConfig} / {@link ContinuousAxisConfig} 多态的存在理由之一确实是
     * 「表达得了连续 X 轴」（见设计文档决策一），于是很容易被理解成「做散点 = 把本方法改宽」。
     *
     * <p>那样做会立刻撞上 {@link #orientation} 已经论证过的同一条矛盾：<b>一张图只有一对共享
     * scale</b>。一个 widget 里只要同时出现「数值对散点层（连×连）」与「柱状层（分×连）」，
     * 同一对轴就满足不了两类要求；数据侧每层各自声明 {@code xAxisProperty} /
     * {@code xAxisBucketing}，而轴是全图共享的。
     *
     * <p>所以正确形态是按 {@link #orientation} 的先例引入<b>图级 chart mode</b>
     * （如 {@code XY} / {@code SCATTER_PAIR}），由本方法按 mode 断言轴性质与图层画法。
     * 换句话说本方法是那条图级规则的正确落点，<b>放开时改的是规则内容，不是取消规则</b>。
     * 届时还需一并实现数值分桶与数值 X 取值 —— 那是一次新功能，不是修正。
     *
     * <p>三条规则（本方法、{@link #orientation}、{@link ChartLayer#xAxisBucketing}）是同一条
     * 事实的三个侧面：<b>一张图只有一对共享 scale</b>。改任何一条前先读 {@link #orientation}
     * 的字段注释。
     *
     * <p>调用点：保存期 {@link DescriptorImpl#verify}（字段级报错、中止写盘）与运行期
     * {@code ChartXYDataProcessor#process}（兜住历史落盘数据或绕过表单的直接调用）。
     *
     * @return {@code null} 表示合法；否则为可直接展示给用户的错误描述
     */
    public String axisKindViolation() {
        if (this.xAxis == null || this.yAxis == null) {
            // 必填项由 Validator.require 兜住；运行期绕过表单时给出可读报错，而不是 NPE
            return "X 轴与 Y 轴的配置都不能为空。";
        }
        if (!(this.xAxis instanceof CategoricalAxisConfig)) {
            return "X 轴必须是分类轴：X 上的取值是各图层按「X 轴属性」分组得到的离散标签"
                    + "（分类键或时间桶），不是原始数值。散点图这类需要数值 X 轴的场景暂未支持。";
        }
        if (!(this.yAxis instanceof ContinuousAxisConfig)) {
            return "Y 轴必须是连续轴：Y 上绘制的是各系列的聚合结果（count / sum / avg 等数值），"
                    + "分类轴在这里无法表达数值刻度。";
        }
        return null;
    }

    private static String layerTypeLabel(AbstractLayerType layerType) {
        if (layerType instanceof LineLayerType) {
            return "折线图";
        }
        if (layerType instanceof ScatterLayerType) {
            return "散点图";
        }
        // BarLayerType 已在上面被放过；走到这里说明是将来新增、尚未声明 supportsHorizontal 的画法
        return "非柱状图画法";
    }

    @TISExtension
    public static class DescriptorImpl extends BaseWidgetDescriptor<IWorkshopWidget> {

        /**
         * 运行时过滤条件的<b>请求参数</b>（不是表单字段），发在 query string 里：
         * {@code {"图层标题": [{"property","operator","value"}]}}。
         *
         * <p>过滤值（{@code ObjectSetFilterValue}）只存在于浏览器内存 —— 它是「某个变量在
         * 当前页面上下文里取什么值」的运行时事实，后端拿不到。因此必须由前端按图层算好、
         * 随 {@code get_data} 一起上送；不带就退化成查全量，看起来只会像「过滤没生效」。
         */
        public static final String KEY_FILTERS_BY_LAYER = "filtersByLayer";

        public static final String METHOD_GET_DATA = "get_data";

        /**
         * 保存期的硬性约束，两条：
         * <ol>
         *   <li>{@code HORIZONTAL} 只能用于「全部图层都是柱状图」的图（{@link #orientation}）。</li>
         *   <li>轴性质必须是「X = 分类轴、Y = 连续轴」（{@link ChartXYWidget#axisKindViolation}）。</li>
         * </ol>
         *
         * <p>为什么两条都必须是图级约束、为什么不能下沉到 {@code BarLayerType} / 轴子类，见
         * {@link ChartXYWidget#orientation} 的字段注释（改之前请先读）。
         *
         * <p>调用链：标准插件保存、{@code module-save} / {@code module-apply} 都经
         * {@code AttrValMap#validateWithScope(..., STRICT)} → {@code Descriptor.load}；
         * 本方法在必填项等基础校验<b>通过之后</b>执行，因此 {@code newInstance()} 一定拿得到
         * 已解析的 {@code layers} / {@code xAxis} / {@code yAxis}。返回 {@code false} 会中止
         * 写盘（module-save 一次写多段实体，不允许半写状态）。
         *
         * <p>两条都检查完再返回：一次保存把所有问题都报出来，避免用户改一条、再存、再遇到下一条。
         */
        @Override
        protected boolean verify(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
            ChartXYWidget widget = postFormVals.newInstance();
            boolean valid = true;

            String orientationViolation = widget.orientationViolation();
            if (orientationViolation != null) {
                msgHandler.addFieldError(context, "orientation", orientationViolation);
                valid = false;
            }
            String axisKindViolation = widget.axisKindViolation();
            if (axisKindViolation != null) {
                // 报错挂在 xAxis 上：多数情况问题出在 X 选了连续轴；Y 选错时两个字段看着都不对，
                // 落到第一个轴字段上至少能定位到「轴那一块」
                msgHandler.addFieldError(context, "xAxis", axisKindViolation);
                valid = false;
            }
            return valid;
        }

        @Override
        protected boolean validateAll(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
            return this.verify(msgHandler, context, postFormVals);
        }

        @Override
        public String getDisplayName() {
            return "Chart XY";
        }

        @Override
        public WorkShopWidgetType getWidgetType() {
            return WorkShopWidgetType.CHART_XY;
        }

        /**
         * 图表的取数入口：前端把整个 widget 的表单投影 POST 上来，这里拉行、分桶、聚合。
         *
         * <p>聚合为什么在服务端做见 {@link ChartXYDataProcessor} 的类注释。
         */
        @Override
        public void httpProcess(IControlMsgHandler paramGetter, IPluginContext pluginContext, Context context) throws Exception {
            String methodType = paramGetter.getString("type");
            if (!METHOD_GET_DATA.equals(methodType)) {
                throw new IllegalStateException("illegal methodType:" + methodType);
            }
            JSONObject postContent = pluginContext.getJSONPostContent();
            // ⚠️ 白名单是唯一决定字段能不能被读到的开关：漏掉 layers，widget.layers 恒为 null 且不报错。
            // 嵌套的 layerType / xAxisBucketing / aggregation 由 createDescribable 递归解析，不必逐一列出。
            // orientation 必须列在这里，否则 ChartXYDataProcessor 里的横向约束校验只能看到字段初值。
            AttrValMap.RelevantFieldsContextAttrValMap<ChartXYWidget> attrs
                    = AttrValMap.createRelevantFieldsContext(paramGetter, context, postContent,
                    (desc) -> List.of("layers", "xAxis", "yAxis", "orientation"));
            if (!attrs.validate().isValid()) {
                return;
            }
            ChartXYWidget widget = attrs.createPluginInstance();

            String filtersJson = paramGetter.getString(KEY_FILTERS_BY_LAYER);
            JSONObject filtersByLayer = StringUtils.isBlank(filtersJson)
                    ? new JSONObject() : JSON.parseObject(filtersJson);
            paramGetter.setBizResult(context, ChartXYDataProcessor.process(widget, filtersByLayer));
        }
    }
}
