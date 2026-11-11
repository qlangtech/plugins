package com.qlangtech.tis.plugin.ontology.workshop.widget.component;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.plugin.ontology.workshop.widget.FilterItemConfig;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.FilterListWidget;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * {@link HistogramComponent#createPayloadProps(List)} 的<b>两类失败的分界</b>。
 *
 * <h3>装配错误 vs 取数失败</h3>
 * 这两类失败的处理方式刻意不同，本类逐条钉住：
 * <ul>
 *   <li><b>装配错误</b>（祖先链不完整：null / 层数不对 / 某层类型不对）→ <b>抛</b>
 *       {@link IllegalStateException}，文案全英文且点名实际收到的链。静默给空载荷会把
 *       「宿主没铺链」伪装成「该属性暂无数据」，排查方向直接跑偏；</li>
 *   <li><b>取数失败</b>（变量为空 / 变量不存在 / 属性不是数值型 / 方言不支持聚合）→ 只给
 *       {@link HistogramComponent#KEY_HISTOGRAM_ERROR}，<b>不抛</b>：最坏后果只能是这张图
 *       画不出来，绝不能连坐整个 module 的下发与保存。</li>
 * </ul>
 * 附加约束：两个键<b>互斥</b>（前端只有一条判断：有 error 显红字 / 有 data 画图）。
 *
 * <p>真正的分桶链路要走数据库（{@code ObjectSetVariable#executeAggregation}），不在单测范围内；
 * 本类跑在<b>没有运行期上下文</b>的环境里，恰好正好覆盖「变量解析必然失败」这条取数失败的降级分支。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/11/5
 */
public class TestHistogramComponentPayload {

    /**
     * 祖先链不完整 → <b>抛</b> {@link IllegalStateException}，不做静默空载荷。
     *
     * <p>四种形态覆盖了所有「不完整」：没有链、链为空、只有宿主、链更长（不是本类预期的
     * 嵌套位置）。第四种尤其重要：多出来的祖先把 {@code size == 2} 这个形状检查打掉，
     * 说明判断依据是<b>形状</b>而不是「至少有这么几个元素」。
     *
     * <p>为什么这里必须是异常而不是 {@code Map.of()}：空载荷在前端就是「该属性暂无可用数据」，
     * 一个装配错误会被伪装成「数据源是空的」，排查方向直接跑偏。
     */
    @Test
    public void testIncompleteParentThrows() {
        HistogramComponent component = histogram();
        FilterListWidget widget = widget("obj_set");
        FilterItemConfig item = row("amount");

        assertInvalidParent(component, null, "parent is null");
        assertInvalidParent(component, List.of(), "exactly 2");
        assertInvalidParent(component, List.of(widget), "exactly 2");
        assertInvalidParent(component, List.of(widget, item, item), "exactly 2");
    }

    /**
     * 链长度对但类型不对 → 同样抛，且文案点名是<b>哪一层</b>类型不对。
     *
     * <p>这条防的是「按位置强转」的写法：长度够了就 {@code (FilterListWidget) parent.get(0)}，
     * 一旦上游换了嵌套层级（比如把 histogram 挪到别的宿主里），会直接 CCE —— 而 CCE 的消息
     * 只说「两个类不兼容」，不说「这是直方图的祖先链约束」，排查得从头翻链路。
     */
    @Test
    public void testWrongParentTypesThrow() {
        HistogramComponent component = histogram();
        FilterItemConfig item = row("amount");

        assertInvalidParent(component, List.of(item, item), "parent[0]");
        assertInvalidParent(component, List.of(widget("obj_set"), histogram()), "parent[1]");
    }

    /**
     * 异常文案必须<b>全是 ASCII</b>：它面向开发者与日志，不是界面文案。
     *
     * <p>混进中文会让线上日志检索（按英文关键字 grep）、以及下游对 message 做断言的测试
     * 都变脆。这里逐条检查，避免以后有人「顺手翻译一下」。
     */
    @Test
    public void testInvalidParentMessagesHaveNoChinese() {
        HistogramComponent component = histogram();
        FilterItemConfig item = row("amount");
        List<List<Describable<?>>> invalidChains = new ArrayList<>();
        invalidChains.add(null);
        invalidChains.add(List.of());
        invalidChains.add(List.of(widget("obj_set")));
        invalidChains.add(List.of(item, item));

        for (List<Describable<?>> parent : invalidChains) {
            IllegalStateException e = catchIllegalState(component, parent);
            Assert.assertNotNull("非法链必须抛 IllegalStateException：" + parent, e);
            String message = e.getMessage();
            Assert.assertNotNull("异常必须带 message", message);
            for (int i = 0; i < message.length(); i++) {
                char c = message.charAt(i);
                Assert.assertTrue("异常信息必须全 ASCII，实际第 " + i + " 个字符是 '" + c + "'：" + message,
                        c < 128);
            }
        }
    }

    /**
     * {@code objectSetVar} 为空 → 只有 {@code histogramError}，且文案非空。
     *
     * <p>这是「配置还没填完」的合法中间态：前端应显示红字，但整页照常渲染。
     */
    @Test
    public void testBlankObjectSetVarDegradesToError() {
        Map<String, Object> payload = histogram().createPayloadProps(
                List.of(widget(null), row("amount")));

        Assert.assertEquals("失败时不能同时出现 histogram 键", 1, payload.size());
        assertErrorOnly(payload);
    }

    /**
     * 变量解析必然失败（本测试没有运行期上下文）→ 同样降级成错误文案，<b>不抛</b>。
     *
     * <p>这条正是生产上「用户把对象集变量删了、页面却整页打不开」的那一幕。若将来
     * {@code WorkshopVariable.loadDetail} 对空上下文改抛不继承 {@code Exception} 的错误
     * （如 {@code AssertionError}），本用例会变红 —— 那是有意的信号：载荷出口必须只吞
     * {@code Exception}，任何 {@code Error} 都不该被它掩盖。
     */
    @Test
    public void testUnresolvableVariableDegradesToError() {
        Map<String, Object> payload = histogram().createPayloadProps(
                List.of(widget("not_exist_object_set_var"), row("amount")));

        Assert.assertEquals("失败时不能同时出现 histogram 键", 1, payload.size());
        assertErrorOnly(payload);
    }

    /**
     * 行 {@code property} 为空 → {@link HistogramDataProcessor} 在解变量<b>之前</b>就拒掉，
     * 同样只能降级。
     */
    @Test
    public void testBlankPropertyDegradesToError() {
        Map<String, Object> payload = histogram().createPayloadProps(
                List.of(widget("obj_set"), row(" ")));

        Assert.assertEquals(1, payload.size());
        assertErrorOnly(payload);
    }

    /**
     * 重复调用返回等价结果。
     *
     * <p>序列化链路一请求内可能走两趟（{@code appendWidgetsJSON} + section），载荷出口
     * 必须可重入。这里刻意<b>不</b>断言「同一对象」，只断语义等价 —— 实现若将来加缓存，
     * 也不该把本用例变红。
     */
    @Test
    public void testRepeatedCallsAreEquivalent() {
        HistogramComponent component = histogram();
        List<Describable<?>> parent = List.of(widget(null), row("amount"));

        Map<String, Object> first = component.createPayloadProps(parent);
        Map<String, Object> second = component.createPayloadProps(parent);
        Assert.assertEquals("两次调用必须给出等价结果", first, second);
        Assert.assertEquals("键集合一致", first.keySet(), second.keySet());
    }

    /**
     * 桶数归一：缺省 / 越界一律回落，不做边界夹取（越界说明配置坏了，夹取会把它伪装成合理配置）。
     */
    @Test
    public void testResolveBucketCount() {
        HistogramComponent component = histogram();

        component.bucketCount = null;
        Assert.assertEquals(HistogramComponent.DFT_BUCKET_COUNT, component.resolveBucketCount());

        component.bucketCount = 0;
        Assert.assertEquals(HistogramComponent.DFT_BUCKET_COUNT, component.resolveBucketCount());

        component.bucketCount = HistogramComponent.MAX_BUCKET_COUNT + 1;
        Assert.assertEquals(HistogramComponent.DFT_BUCKET_COUNT, component.resolveBucketCount());

        component.bucketCount = 20;
        Assert.assertEquals(20, component.resolveBucketCount());
    }

    /**
     * 非法祖先链的统一断言：必须抛 {@link IllegalStateException}，且 message 含
     * {@code contains} 指定的定位信息。
     */
    private static void assertInvalidParent(HistogramComponent component,
                                            List<Describable<?>> parent, String contains) {
        IllegalStateException e = catchIllegalState(component, parent);
        Assert.assertNotNull("非法祖先链 " + parent + " 必须抛 IllegalStateException，而不是返回空载荷", e);
        Assert.assertTrue("异常信息必须点名问题所在（期望含 \"" + contains + "\"），实际："
                + e.getMessage(), e.getMessage().contains(contains));
    }

    /** 只接 {@link IllegalStateException}；其它异常类型直接让用例失败 */
    private static IllegalStateException catchIllegalState(HistogramComponent component,
                                                           List<Describable<?>> parent) {
        try {
            component.createPayloadProps(parent);
            return null;
        } catch (IllegalStateException e) {
            return e;
        } catch (RuntimeException e) {
            Assert.fail("应该抛 IllegalStateException，实际抛了 " + e.getClass().getName() + "：" + e.getMessage());
            return null;   // unreachable
        }
    }

    /**
     * 「只带 histogramError、且文案非空」的唯一校验口径 —— 三个失败用例共用。
     */
    private static void assertErrorOnly(Map<String, Object> payload) {
        Assert.assertFalse("失败载荷里不能出现 " + HistogramComponent.KEY_HISTOGRAM,
                payload.containsKey(HistogramComponent.KEY_HISTOGRAM));
        Object error = payload.get(HistogramComponent.KEY_HISTOGRAM_ERROR);
        Assert.assertTrue("失败载荷必须有 " + HistogramComponent.KEY_HISTOGRAM_ERROR + "，实际：" + payload,
                error instanceof String);
        Assert.assertFalse("错误文案不能是空白（前端会把它当红字直接显示）",
                ((String) error).isBlank());
    }

    private static HistogramComponent histogram() {
        return new HistogramComponent();
    }

    private static FilterListWidget widget(String objectSetVar) {
        FilterListWidget widget = new FilterListWidget();
        widget.id = "w-filter-list";
        widget.objectSetVar = objectSetVar;
        return widget;
    }

    private static FilterItemConfig row(String property) {
        FilterItemConfig row = new FilterItemConfig();
        row.property = property;
        row.label = "金额";
        row.component = histogram();
        return row;
    }
}
