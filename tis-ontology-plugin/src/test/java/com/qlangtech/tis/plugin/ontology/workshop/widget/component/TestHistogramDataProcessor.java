package com.qlangtech.tis.plugin.ontology.workshop.widget.component;

import com.alibaba.citrus.turbine.impl.DefaultContext;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.FilterListWidget;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * {@link HistogramDataProcessor} 里两条「算错了也不会报错、只是图长错」的纯逻辑。
 *
 * <h3>为什么挑这两条</h3>
 * <ul>
 *   <li>{@link HistogramDataProcessor#foldCounts}：决定「请求 10 根柱子是不是真得到 10 根」。
 *       算错一格，末桶会吞掉倒数第二桶的数据，而图看起来完全正常；</li>
 *   <li>{@code httpProcess} 的分派顺序：未知 {@code type} 必须在<b>读请求体之前</b>被挡下。
 *       反过来的话，前端把 {@code get_options} 拼成 {@code get_optoins} 时会先撞上一个
 *       与拼写无关的 NPE，排查方向就被带偏了。</li>
 * </ul>
 *
 * <p>完整的分桶链路要走数据库（{@code ObjectSetVariable#executeAggregation}），不在单测范围内
 * —— 这里只钉住不依赖 IO 的那一段。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/11/4
 */
public class TestHistogramDataProcessor {

    /**
     * 分组索引 → 桶下标。末桶折叠是刻意的（见 {@code HistogramDataProcessor} 类注释）。
     *
     * <p>表里第 3、4、5 行是同一条规则的三种越界形态：{@code floor(max/width) - floor(min/width)}
     * 恒等于 {@code bucketCount}，所以真实数据只会落到 {@code slot == bucketCount}；更大的值
     * 只可能来自「两次查询之间数据变了」，同样必须折进末桶而不是数组越界。
     */
    @Test
    public void testFoldCounts() {
        final int buckets = 10;
        Assert.assertEquals("第 0 桶", 0, HistogramDataProcessor.foldCounts(0, buckets));
        Assert.assertEquals("中间桶原样映射", 5, HistogramDataProcessor.foldCounts(5, buckets));
        Assert.assertEquals("末桶", 9, HistogramDataProcessor.foldCounts(9, buckets));
        Assert.assertEquals("越界一格 → 折进末桶", 9, HistogramDataProcessor.foldCounts(10, buckets));
        Assert.assertEquals("越界多格 → 折进末桶", 9, HistogramDataProcessor.foldCounts(9999, buckets));
        Assert.assertEquals("负索引（仅可能来自数据漂移）→ 首桶", 0, HistogramDataProcessor.foldCounts(-1, buckets));

        Assert.assertEquals("单桶配置下一切索引都折进第 0 桶",
                0, HistogramDataProcessor.foldCounts(7, 1));
    }

    /**
     * 未知 {@code type} 在<b>读任何载荷之前</b>就抛 —— 这是拼错端点名时唯一能定位问题的信号。
     *
     * <p>用一个 {@code getJSONPostContent()} 返回 null 的上下文：分派若发生在读请求体之后，
     * 这里会变成 {@code NullPointerException: postContent can not be null}，断言随之失败。
     *
     * <p>⚠️ {@code get_histogram} 也在这一批里：它已经<b>不是</b>合法端点（直方图数据改由
     * {@code HistogramComponent#createPayloadProps} 随 {@code appendVals} 下发）。
     * 留着这条断言是为了防止有人把它加回去，而前端仍按旧的 query 参数调用。
     */
    @Test
    public void testUnknownMethodTypeFailsBeforeReadingPostContent() {
        FilterListWidget.DescriptorImpl descriptor = new FilterListWidget.DescriptorImpl();

        for (String typo : new String[]{"get_histogram", "get_histgram", "get_property_option", "", "get_data"}) {
            IControlMsgHandler paramGetter = Mockito.mock(IControlMsgHandler.class);
            Mockito.doReturn(typo).when(paramGetter).getString("type");
            try {
                descriptor.httpProcess(paramGetter, null, new DefaultContext());
                Assert.fail("未知 type:\"" + typo + "\" 必须被挡下");
            } catch (IllegalStateException e) {
                Assert.assertTrue("报错必须点名非法的 type，实际：" + e.getMessage(),
                        e.getMessage().contains("illegal methodType"));
            } catch (Exception e) {
                Assert.fail("未知 type:\"" + typo + "\" 应该抛 IllegalStateException 而不是 "
                        + e.getClass().getSimpleName() + ":" + e.getMessage());
            }
        }
    }

    /**
     * 两个端点名互不相同 —— 改名时漏改一处，会把两个端点接到同一个分支上（且不报错）。
     */
    @Test
    public void testEndpointNamesAreDistinct() {
        Assert.assertNotEquals(FilterListWidget.HTTP_METHOD_GET_OPTIONS,
                FilterListWidget.HTTP_METHOD_GET_PROPERTY_OPTIONS);
    }
}
