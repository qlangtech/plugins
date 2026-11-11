package com.qlangtech.tis.plugin.ontology.workshop.widget.component;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.ibatis.RowMap;
import com.qlangtech.tis.manage.common.Option;
import com.qlangtech.tis.plugin.ontology.aggregation.AggregationCriteria;
import com.qlangtech.tis.plugin.ontology.impl.action.widget.WidgetOptionConfig;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * 候选项来源的 <b>GET 载荷（appendVals）</b>契约 —— 后端
 * {@link AbstractOpsSource#createPayloadProps()} 的线格式。
 *
 * <p>这里的失败模式全是<b>静默</b>的：载荷键名对不上时前端只是「候选全空」，接口照常 200，
 * 没有任何异常可以观察到。故把外层键名与行键名逐字钉住。
 *
 * <p>为什么直接测 {@code createPayloadProps()} 而不是某个 resolver：候选项的权威出口就是
 * {@link com.qlangtech.tis.util.DescribableJSON.AppendPayloadAttribute} ——
 * 多态子类的 {@code createOptions()} 由序列化链路调用，没有外部编排者。
 *
 * <p>只测「手填」与「对象集未配置 / 解析失败」三类：{@code object-set} 的<b>成功路径</b>要经
 * {@code WorkshopVariable.loadDetail} 走插件存储并真的查库，需要 TIS 运行期上下文，
 * 不属于本类的纯逻辑部分。失败路径恰好可以在没有上下文时稳定复现（见对应用例）。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/11/3
 */
public class TestOpsSourceOptions {

    /**
     * 手填候选项：载荷必须是 {@code {"options": [{label, val}]}} —— 外层键名由
     * {@link Descriptor#KEY_OPTIONS} 决定，行键名由 {@link Option#toJson(List)} 的
     * {@code KEY_LABEL} / {@code KEY_VALUE} 决定。
     *
     * <p>前端 {@code toFilterOptionItems} 读的也是 {@code val}：这一处曾经是 {@code value}，
     * 两边各自手拼键名，改一边另一边静默失效。
     */
    @Test
    public void testManualOptionsWireFormat() {
        ManualOpsSource manual = manual(option("dept_a", "部门A"), option("dept_b", "部门B"));

        JSONArray options = optionsOf(manual.createPayloadProps(Collections.emptyList()));
        Assert.assertEquals(2, options.size());

        JSONObject first = options.getJSONObject(0);
        Assert.assertEquals("部门A", first.getString(Option.KEY_LABEL));
        Assert.assertEquals("dept_a", first.getString(Option.KEY_VALUE));
        Assert.assertNull("未设置的字段不能被 toJson 带出来（前端会把多余的键当成配置）",
                first.get(Option.KEY_END_TYPE));
    }

    /**
     * {@code label} 留空回落 {@code val}：「有值没字」的选项最容易在页面上被当成空白项。
     */
    @Test
    public void testManualLabelFallsBackToVal() {
        JSONArray options = optionsOf(manual(option("dept_a", null)).createPayloadProps(Collections.emptyList()));
        JSONObject option = options.getJSONObject(0);
        Assert.assertEquals("label 缺失时必须回落 val", "dept_a", option.getString(Option.KEY_LABEL));
        Assert.assertEquals("dept_a", option.getString(Option.KEY_VALUE));
    }

    /**
     * 按 {@code val} 去重、跳过空白行：前端子表单以 {@code val} 作行键，同值行在渲染期就被
     * 静默合并；存量数据（直接改过 XML 的）仍可能带重复项，这里再挡一次。
     */
    @Test
    public void testManualDedupeAndBlankSkip() {
        JSONArray options = optionsOf(manual(
                option("dept_a", "部门A"),
                option("dept_a", "部门A(重复)"),
                option("  ", "空白值"),
                option(null, "没有值")).createPayloadProps(Collections.emptyList()));

        Assert.assertEquals("重复 val 与空白 val 都要挡掉", 1, options.size());
        Assert.assertEquals("保留首次出现的那条", "部门A", options.getJSONObject(0).getString(Option.KEY_LABEL));
    }

    /**
     * 对象集来源但还没选变量：合法的中间态，降级成空列表，<b>载荷仍然存在</b>
     * （外层键必须在：前端按 {@code appendVals.options} 取值，缺键与「没有候选」是两条不同的分支）。
     */
    @Test
    public void testObjectSetWithoutVariableYieldsEmptyOptions() {
        Map<String, Object> payload = new ObjectSetOpsSource().createPayloadProps(Collections.emptyList());
        Assert.assertTrue("外层键必须逐字是 Descriptor.KEY_OPTIONS",
                payload.containsKey(Descriptor.KEY_OPTIONS));
        Assert.assertTrue("未选变量的对象集来源应回空数组", optionsOf(payload).isEmpty());
    }

    /**
     * 对象集来源取数失败（变量不存在 / 无运行期上下文）：<b>不得抛</b>，降级成空列表。
     *
     * <p>这条是本次下沉最关键的行为契约：候选项现在在 module GET 的序列化链路里算，
     * 一次异常会中止<b>整个页面</b>的下发（POST 侧序列化同样会调到它，还会连带保存失败）。
     */
    @Test
    public void testObjectSetResolutionFailureDegradesToEmpty() {
        ObjectSetOpsSource objectSet = new ObjectSetOpsSource();
        objectSet.sourceObjectSetVar = "not_exist_object_set_var";
        // 不能 try/catch 后 assertTrue(true)：抛出去 JUnit 会直接判失败，这正是想要的
        Map<String, Object> payload = objectSet.createPayloadProps(Collections.emptyList());
        Assert.assertTrue("解析失败必须降级成空列表，而不是冒泡", optionsOf(payload).isEmpty());
    }

    /**
     * {@code createPayloadProps()} 的值不能是 null —— {@code Map.of} 遇 null 直接 NPE，
     * 而它在序列化链路里，NPE 会连带整个 module 下发失败。
     */
    @Test
    public void testPayloadNeverNull() {
        List<AbstractOpsSource> sources = new ArrayList<>();
        ManualOpsSource manual = new ManualOpsSource();
        // options 保持 null（XStream 反序列化不走字段初始化器，存量数据可能是 null）
        sources.add(manual);
        ManualOpsSource emptyManual = new ManualOpsSource();
        emptyManual.options = Collections.emptyList();
        sources.add(emptyManual);
        sources.add(new ObjectSetOpsSource());

        for (AbstractOpsSource source : sources) {
            Map<String, Object> payload = source.createPayloadProps(Collections.emptyList());
            Assert.assertNotNull(source.getClass().getSimpleName() + " 的载荷不能为 null", payload);
            Assert.assertNotNull(source.getClass().getSimpleName() + " 的 options 不能为 null",
                    payload.get(Descriptor.KEY_OPTIONS));
        }
    }

    // ==================================================================
    //  对象集来源：计数 + 空值行（buildOptions 的纯转换部分）
    // ==================================================================

    /**
     * 按计数<b>降序</b>排，空值行固定排在最后。
     *
     * <p>降序不是为了好看：折叠成 5 行时，看不到的那一堆必须是<b>长尾</b>，
     * 否则「默认折叠」等于把最有用的几个候选藏起来。
     */
    @Test
    public void buildOptionsSortsByCountDescAndAppendsNullRowLast() {
        List<Option> options = ObjectSetOpsSource.buildOptions(List.of(
                groupRow("unable", 2L),
                groupRow("done", 230L),
                groupRow(null, 1L),
                groupRow("blocked", 17L)), true);

        Assert.assertEquals("3 个真实候选 + 1 个空值行", 4, options.size());
        Assert.assertEquals("done", options.get(0).getValue());
        Assert.assertEquals("blocked", options.get(1).getValue());
        Assert.assertEquals("unable", options.get(2).getValue());

        Option nullRow = options.get(3);
        Assert.assertEquals(ObjectSetOpsSource.NULL_OPTION_LABEL, nullRow.getName());
        Assert.assertTrue("空值行必须被标记：前端据此把值翻译成 isNull / inOrNull，"
                + "否则哨兵值会被当成一个真实的维度值发给下游", nullRow instanceof OptionWithCount count && count.isNullValue());
    }

    /**
     * 空值行的计数是<b>累加</b>：label 与 value 是两个属性时，{@code (label, value)}
     * 可能有多行落在 value 为空上，它们对使用者来说是同一个候选。
     */
    @Test
    public void buildOptionsAccumulatesNullRowCountAcrossLabels() {
        List<Option> options = ObjectSetOpsSource.buildOptions(List.of(
                twoColRow("未知来源", null, 3L),
                twoColRow("未填写", null, 4L),
                twoColRow("正常", "A", 10L)), false);

        Assert.assertEquals(2, options.size());
        OptionWithCount nullRow = (OptionWithCount) options.get(1);
        Assert.assertEquals("两条 label 不同的空值行要并成一条", 7L, nullRow.getCount());
        Assert.assertEquals(10L, ((OptionWithCount) options.get(0)).getCount());
        Assert.assertEquals("A", options.get(0).getValue());
    }

    /** 没有空值分组就不产出空值行 —— 等价于「该属性确实存在 NULL」，不需要额外的可空性元信息。 */
    @Test
    public void buildOptionsOmitsNullRowWhenNoNullGroup() {
        List<Option> options = ObjectSetOpsSource.buildOptions(List.of(groupRow("A", 1L)), true);
        Assert.assertEquals(1, options.size());
        Assert.assertTrue(options.stream().noneMatch(o -> o instanceof OptionWithCount c && c.isNullValue()));
    }

    /**
     * 计数随候选项一起下发：前端拿不到 {@code count} 时只是不画分布条，
     * 不报错、也看不出来 —— 故把键名钉在 {@link OptionWithCount#KEY_COUNT}。
     */
    @Test
    public void buildOptionsCarriesCountIntoWireFormat() {
        // 走真实的序列化出口：OptionWithCount 覆写的 appendExtraProps 必须真的出现在 toJson 里
        JSONArray options = Option.toJson(
                ObjectSetOpsSource.buildOptions(List.of(groupRow("dept_a", 42L)), true));

        JSONObject row = options.getJSONObject(0);
        Assert.assertEquals("dept_a", row.getString(Option.KEY_VALUE));
        Assert.assertEquals(42, row.getIntValue(OptionWithCount.KEY_COUNT));
        Assert.assertNull("真实候选不带 nullValue 键（前端会把 true 当成空值行）",
                row.get(OptionWithCount.KEY_NULL_VALUE));
    }

    /** 空值行的线格式：哨兵 val + {@code nullValue: true} + 累加后的计数。 */
    @Test
    public void buildOptionsNullRowWireFormat() {
        JSONArray options = Option.toJson(ObjectSetOpsSource.buildOptions(List.of(groupRow(null, 5L)), true));

        JSONObject row = options.getJSONObject(0);
        Assert.assertEquals(OptionWithCount.NULL_OPTION_VALUE, row.getString(Option.KEY_VALUE));
        Assert.assertEquals(ObjectSetOpsSource.NULL_OPTION_LABEL, row.getString(Option.KEY_LABEL));
        Assert.assertTrue(row.getBooleanValue(OptionWithCount.KEY_NULL_VALUE));
        Assert.assertEquals(5, row.getIntValue(OptionWithCount.KEY_COUNT));
    }

    /** 聚合法分组行：{@code (c0)} 是分组值，{@code m0} 是计数 */
    private static RowMap groupRow(Object value, long count) {
        RowMap row = new RowMap();
        row.put(AggregationCriteria.groupAlias(0), value);
        row.put(AggregationCriteria.metricAlias(0), count);
        return row;
    }

    /** label / value 为两个属性时的分组行 */
    private static RowMap twoColRow(Object label, Object value, long count) {
        RowMap row = new RowMap();
        row.put(AggregationCriteria.groupAlias(0), label);
        row.put(AggregationCriteria.groupAlias(1), value);
        row.put(AggregationCriteria.metricAlias(0), count);
        return row;
    }

    // ==================================================================

    /** 取载荷里的 {@code options} 数组（键名由 {@link Descriptor#KEY_OPTIONS} 决定） */
    private static JSONArray optionsOf(Map<String, Object> payload) {
        JSONArray options = (JSONArray) payload.get(Descriptor.KEY_OPTIONS);
        Assert.assertNotNull("载荷缺 options 键（后端 Descriptor.KEY_OPTIONS）", options);
        return options;
    }

    private static ManualOpsSource manual(WidgetOptionConfig... options) {
        ManualOpsSource manual = new ManualOpsSource();
        manual.options = List.of(options);
        return manual;
    }

    private static WidgetOptionConfig option(String val, String label) {
        WidgetOptionConfig option = new WidgetOptionConfig();
        option.val = val;
        option.label = label;
        return option;
    }
}
