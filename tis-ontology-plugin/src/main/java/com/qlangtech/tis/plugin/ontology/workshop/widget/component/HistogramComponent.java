package com.qlangtech.tis.plugin.ontology.workshop.widget.component;

import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.ontology.workshop.widget.FilterItemConfig;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.FilterListWidget;
import com.qlangtech.tis.util.DescribableJSON;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 直方图（Histogram）过滤控件：把<b>数值属性</b>的分布画成柱状图，点柱子即按该区间过滤。
 *
 * <h3>为什么这是「控件形态」而不是「一个新的 widget」</h3>
 * 它和 keyword / date-picker 是同一层的东西：一行过滤项在页面上长什么样。数据仍然由
 * {@code FilterListWidget} 输出（两条谓词 {@code >= lo} / {@code < hi}），下游消费者完全
 * 不需要知道这个区间是从柱子上点出来的。
 *
 * <h3>分桶为什么必须由服务端算</h3>
 * 直方图的桶是「按区间计数」，不是「按原始值去重计数」。在浏览器里算就必须先拿到
 * <b>全部明细值</b>，那在大表上直接不可行（也会被 {@code maxGroups} 截断成一份残缺分布）。
 * 因此由服务端做两趟聚合：第一趟取 {@code MIN/MAX} 定桶宽，第二趟按
 * {@code FLOOR(col / width)} 分组计数（见 {@link HistogramDataProcessor}）。
 *
 * <h3>分布数据为什么走 {@link DescribableJSON.AppendPayloadAttribute}</h3>
 * 与候选项（{@link AbstractOpsSource}）同一套理由：直方图<b>不是落盘配置</b>，而是读时现算的
 * 快照（要查两次库）。放进 {@code vals} 会破坏「{@code vals} 就是可回传配置」这条语义；
 * 单独开一条 {@code get_histogram} 端点，则要再维护一套请求构造、字段白名单与前端投影 ——
 * 而且每行一次往返。
 * <ul>
 *   <li><b>主链路</b>：module GET 序列化时随 item 信封的 {@code appendVals.histogram} 一并下发。
 *       此时祖先链是 {@code [FilterListWidget, FilterItemConfig]} —— 计算所需的
 *       {@code objectSetVar} 与行 {@code property} 都在里面，桶数由本实例自给；</li>
 *   <li><b>补偿链路</b>：配置面板编辑过之后，前端 {@code Item.project()} 会把 {@code appendVals}
 *       丢掉，由 {@code FilterListWidget} 的 {@code get_options} 端点重新序列化一次 ——
 *       两条链路走的是<b>同一段</b> {@code getItemJson()}，故键名不可能分叉。</li>
 * </ul>
 *
 * <h3>为什么没有 min / max 字段</h3>
 * 区间边界<b>由数据决定</b>：同一份配置换一张表、或者表里数据长了一倍，硬编码的区间就会
 * 一半是空的。想收窄范围用别的手段（对象集自身的静态过滤条件），而不是让构建者手填边界
 * —— 手填的边界一旦与数据脱节，用户点柱子的结果就是「查不到东西」，还看不出原因。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/11/4
 * @see HistogramDataProcessor
 */
public class HistogramComponent extends FilterComponent
        implements DescribableJSON.AppendPayloadAttribute, Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 载荷键：分布数据本体，形状见 {@link HistogramDataProcessor} 的「响应契约」。
     * 键名即线格式，前端 {@code widget-config.model.ts} 按字符串取，两侧必须逐字一致。
     */
    public static final String KEY_HISTOGRAM = "histogram";

    /**
     * 载荷键：分布算不出来时的错误文案（与 {@link #KEY_HISTOGRAM} <b>互斥</b>）。
     *
     * <p>为什么不在失败时直接给 {@code histogram: null}：{@code Map.of} 遇 null 直接 NPE，
     * 而这里抛出去会中止整个 module 的下发与保存（见 {@link DescribableJSON.AppendPayloadAttribute}
     * 的实现契约）。两个键互斥也让前端的判断只有一条：有 error 就显示红字，有 data 就画图。
     */
    public static final String KEY_HISTOGRAM_ERROR = "histogramError";

    /**
     * 柱数（桶数）。
     *
     * <p>Java 初值与 {@code HistogramComponent.json} 的 {@code "dftVal": 10} 是一对
     * （见 {@code TestWorkshopFormDefaults}）：前端新建表单只认 {@code eprops.dftVal}，
     * Java 初始化器它不读；而单测又要求两者逐字一致。
     *
     * <p>上限 100 是刻意的：一步之内能画出来的柱子再多也读不出分布，而且每个柱子都是一次
     * 点击目标，太细反而选不准。
     */
    @FormField(ordinal = 10, type = FormFieldType.INT_NUMBER)
    public Integer bucketCount = 10;

    /** 桶数的合法区间（服务端与前端共用同一份口径） */
    public static final int MIN_BUCKET_COUNT = 1;
    public static final int MAX_BUCKET_COUNT = 100;
    public static final int DFT_BUCKET_COUNT = 10;

    /**
     * 分布数据的载荷出口。
     *
     * <p>祖先链必须<b>完整</b>：{@code [FilterListWidget, FilterItemConfig]}。本类靠它拿
     * {@code widget.objectSetVar}（查哪个对象集）与 {@code item.property}（画哪个属性），
     * 两样缺一不可。<b>链不完整是编程错误，直接抛</b>（{@link IllegalStateException}），
     * 不做「静默空载荷」的降级：
     * <ul>
     *   <li>空载荷在前端就是「该属性暂无可用数据」，一个装配错误会被伪装成「数据源是空的」，
     *       排查方向直接跑偏；</li>
     *   <li>也绝不能退化成「用猜出来的属性去查库」—— 那会画出<b>别的属性</b>的分布，
     *       看起来完全正常，只是数据是错的。</li>
     * </ul>
     * 异常文案一律英文（面向开发者/日志，不是给最终用户看的界面文案），且必须点名实际收到的
     * 链长与每一层的类型，便于一眼定位是哪一层宿主没铺链。
     *
     * <p>⚠️ 这与 {@link DescribableJSON.AppendPayloadAttribute} 接口注释里「不得抛异常」的
     * 一般性建议<b>有意不同</b>：那条建议是给「取数失败」准备的（本类对应
     * {@link #KEY_HISTOGRAM_ERROR} 分支）。装配错误与取数失败是两类事，前者必须响。
     *
     * <p><b>取数失败</b>（属性不在本体里 / 不是数值型 / 变量不存在 / 方言不支持聚合 …）
     * 仍然一律降级成 {@link #KEY_HISTOGRAM_ERROR}：最坏后果只能是「这张图画不出来」，
     * 绝不能是「整个页面打不开」。
     *
     * <p>刻意<b>不做</b>实例级缓存：两趟聚合的代价确实不轻，但缓存要求「配置不可变」，
     * 而序列化路径上拿到的 {@code parent[0] / parent[1]} 与 {@code this.bucketCount}
     * 都不是本实例能保证的稳定输入（重建 / 反序列化 / 面板编辑各走各的对象）。
     * 缓存错一次的代价是「图永远画的是旧配置」，比多查一次库糟得多。
     *
     * @throws IllegalStateException 祖先链不完整（null / 层数不对 / 某层类型不对）
     */
    @Override
    public Map<String, Object> createPayloadProps(List<Describable<?>> parent) {
        if (parent == null) {
            throw new IllegalStateException("histogram payload requires an ancestor chain "
                    + "[FilterListWidget, FilterItemConfig], but parent is null");
        }
        if (parent.size() != 2) {
            throw new IllegalStateException("histogram payload requires ancestor chain "
                    + "[FilterListWidget, FilterItemConfig] with exactly 2 elements, but got "
                    + parent.size() + ": " + describeChain(parent));
        }
        if (!(parent.get(0) instanceof FilterListWidget widget)) {
            throw new IllegalStateException("histogram payload requires parent[0] to be FilterListWidget, but got "
                    + classNameOf(parent.get(0)) + ", chain: " + describeChain(parent));
        }
        if (!(parent.get(1) instanceof FilterItemConfig item)) {
            throw new IllegalStateException("histogram payload requires parent[1] to be FilterItemConfig, but got "
                    + classNameOf(parent.get(1)) + ", chain: " + describeChain(parent));
        }
        try {
            JSONObject histogram = HistogramDataProcessor.process(widget, item.property, resolveBucketCount());
            return Map.of(KEY_HISTOGRAM, histogram);
        } catch (Exception e) {
            // 取数失败与装配错误是两回事：见方法注释
            return Map.of(KEY_HISTOGRAM_ERROR, describe(e));
        }
    }

    /** 祖先链的人读形态（{@code [FilterListWidget, FilterItemConfig]}），仅用于异常文案 */
    private static String describeChain(List<Describable<?>> parent) {
        return parent.stream().map(HistogramComponent::classNameOf)
                .collect(Collectors.joining(", ", "[", "]"));
    }

    /** 只取简单类名：`com.qlangtech...FilterListWidget` 的包名对定位没有信息量 */
    private static String classNameOf(Object elem) {
        return elem == null ? "null" : elem.getClass().getSimpleName();
    }

    /**
     * 归一后的桶数：缺省 / 越界一律回落到 {@link #DFT_BUCKET_COUNT}。
     *
     * <p>不做「夹取到边界」而是整值回落：越界说明配置坏了（不是「想多要一点」），
     * 按边界夹取会把一个明显错误的配置伪装成一个合理的配置。
     */
    public int resolveBucketCount() {
        if (this.bucketCount == null
                || this.bucketCount < MIN_BUCKET_COUNT || this.bucketCount > MAX_BUCKET_COUNT) {
            return DFT_BUCKET_COUNT;
        }
        return this.bucketCount;
    }

    /**
     * 失败文案。取 {@code getMessage()} 而不是 {@code toString()}：前端会把它当红字直接显示，
     * 类名对使用者没有信息量。
     */
    private static String describe(Exception e) {
        String msg = e.getMessage();
        return msg == null || msg.isBlank() ? e.getClass().getSimpleName() : msg;
    }

    @TISExtension
    public static class DefaultDescriptor extends BasicDescriptor implements DescriptorUseableShortComment {

        @Override
        public String getDisplayName() {
            // 线格式 token，前端 deriveKebab 成 'histogram' 并据此分派渲染分支
            return "histogram";
        }

        @Override
        public String shortComment() {
            return "直方图：按数值区间分布筛选";
        }
    }
}
