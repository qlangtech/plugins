package com.qlangtech.tis.plugin.ontology.workshop.widget.impl;

import com.alibaba.citrus.turbine.impl.DefaultContext;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.manage.common.Option;
import com.qlangtech.tis.plugin.ontology.impl.action.widget.WidgetOptionConfig;
import com.qlangtech.tis.plugin.ontology.workshop.widget.FilterItemConfig;
import com.qlangtech.tis.plugin.ontology.workshop.widget.component.KeywordComponent;
import com.qlangtech.tis.plugin.ontology.workshop.widget.component.ManualOpsSource;
import com.qlangtech.tis.plugin.ontology.workshop.widget.component.MultiSelectComponent;
import com.qlangtech.tis.plugin.ontology.workshop.widget.component.ObjectSetOpsSource;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.util.AttrValMap;
import com.qlangtech.tis.util.IPluginContext;
import com.qlangtech.tis.util.PartialSettedPluginContext;
import com.qlangtech.tis.util.UploadPluginMeta;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.Objects;

/**
 * {@code get_options} 补偿端点的<b>返回体形状</b>契约 —— 后端
 * {@link FilterListWidget.DescriptorImpl#getItemJson(FilterListWidget)} 的线格式。
 *
 * <h3>为什么需要这个测试</h3>
 * 前端只读响应里<b>一条深路径</b>：
 * <pre>
 * bizresult.vals.filters[i].vals.component.vals.optionsSource.appendVals.options
 * </pre>
 * 这条路径上任何一层名字写错（或 {@code appendVals} 落到了 {@code vals} 里面）都<b>不会报错</b>
 * —— 请求 200、前端拿到空数组、下拉里没有候选。所以把每一层的键名逐字钉住。
 *
 * <h3>为什么要专门测「补偿链路」</h3>
 * 主链路（module GET）与补偿端点最终都走 {@code DescribableJSON} 的同一段遍历，但入口不同：
 * 补偿端点拿到的是<b>前端 POST 上来的表单投影</b>重建的 widget（{@code AttrValMap} 解析），
 * 而非从落盘配置加载的。如果端点偷懒把 POST body 原样回传，{@code appendVals} 就永远不存在
 * —— 前端投影时已经把它丢了。本类断言的是「端点确实用实例重新序列化了一遍」。
 *
 * <h3>运行环境</h3>
 * 序列化链路经 {@code RootFormProperties#getInstancePropsJson}，它有两处运行期依赖：
 * <ol>
 *   <li>每个描述符字段调 {@code Describable#getDescriptor()} -> {@code TIS.get().getDescriptor(clazz)}；</li>
 *   <li>有级联管道时读 {@code IPluginContext.getThreadLocalInstance()} 的 context 取
 *       {@link UploadPluginMeta}（{@code FilterListWidget} 就有一条
 *       {@code objectSetVar -> filters[].property} 的管道）。</li>
 * </ol>
 * 第二处<b>没有可降级分支</b>：thread-local 为空直接抛。所以 {@link #setUp()} 里铺一个最小
 * 上下文 —— 这不是给测试开的后门，生产路径（{@code plugin_action} 的 action 线程）本来就铺了
 * 这些东西，缺了它 {@code httpProcess} 同样跑不通。
 *
 * <p>第一处为什么通常也能过：本工程 {@code target/classes} 里的 sezpoz 索引在构建期生成，
 * {@code TIS.get()} 在没有任何插件目录配置的环境下也能起来（{@code loadTasks()} 是空图）。
 * JDK17 下反射读 {@code clazz} 需要 {@code --add-opens java.base/java.lang=ALL-UNNAMED}
 * （见 {@code project_jdk17_add_opens_descriptor_scan}）—— 缺了它本类是<b>报错</b>而不是静默变绿。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/11/3
 */
public class TestFilterListOptionsEndpoint {

    private FilterListWidget.DescriptorImpl descriptor;

    private PartialSettedPluginContext pluginContext;

    @Before
    public void setUp() {
        pluginContext = new PartialSettedPluginContext();
        DefaultContext context = new DefaultContext();
        // 级联管道读 UploadPluginMeta.createPluginMeta(context)；没有它就是 NPE（无降级分支）
        UploadPluginMeta.putPluginMeta(context, UploadPluginMeta.parse("widget:require"));
        pluginContext.setContext(context);
        IPluginContext.setPluginContext(pluginContext);

        this.descriptor = new FilterListWidget.DescriptorImpl();
    }

    @After
    public void tearDown() {
        // thread-local 不清会污染同 JVM 的其他测试（surefire 默认复用 fork）
        IPluginContext.setPluginContext(null);
    }

    // ===================================================================
    //  核心：候选落在嵌套 optionsSource 的 appendVals 上
    // ===================================================================

    /**
     * 手填候选 -> {@code filters[0].vals.component.vals.optionsSource.appendVals.options}。
     *
     * <p>四层嵌套一次断言到底：少摊一层（例如读到 {@code component.appendVals}）在 TS 里
     * 只是一个 `undefined`，不报错、只是候选全空 —— 这正是本用例存在的理由。
     */
    @Test
    public void testOptionsLandOnNestedOptionsSource() throws Exception {
        JSONObject item = descriptor.getItemJson(widget(
                row("department", "部门", multiSelect(manual(option("dept_a", "部门A"), option("dept_b", "部门B"))))));

        JSONObject source = optionsSourceEnvelope(item, 0);
        Assert.assertEquals("候选项来源的 impl 是 FQCN（前端按它反推短 key）",
                ManualOpsSource.class.getName(), source.getString("impl"));

        JSONArray options = optionsOf(source);
        Assert.assertEquals("两条手填候选都要下发", 2, options.size());
        JSONObject first = options.getJSONObject(0);
        Assert.assertEquals("行键名是 Option.KEY_LABEL", "部门A", first.getString(Option.KEY_LABEL));
        Assert.assertEquals("行键名是 Option.KEY_VALUE（不是 value）",
                "dept_a", first.getString(Option.KEY_VALUE));
    }

    /**
     * {@code appendVals} 与 {@code vals} <b>同级</b> —— 不是 {@code vals} 里的一个字段。
     *
     * <p>放错层级的后果：前端 {@code resolveFilterOpsSource} 在信封层读 {@code raw['appendVals']}
     * 读不到，会一直当成「没下发」而反复补请求 —— 连 warn 都没有。
     */
    @Test
    public void testAppendValsIsSiblingOfVals() throws Exception {
        JSONObject item = descriptor.getItemJson(widget(
                row("department", "部门", multiSelect(manual(option("dept_a", "部门A"))))));

        JSONObject source = optionsSourceEnvelope(item, 0);
        Assert.assertTrue("appendVals 必须在信封层", source.containsKey(AttrValMap.PLUGIN_APPEND_VALS));
        Assert.assertNull("appendVals 不能在 vals 里", source.getJSONObject(AttrValMap.PLUGIN_EXTENSION_VALS)
                .get(AttrValMap.PLUGIN_APPEND_VALS));
        // 落盘配置仍在 vals 里 —— 候选是附加载荷，不是配置的替代品
        Assert.assertNotNull("optionsSource 的落盘配置仍在 vals 里",
                source.getJSONObject(AttrValMap.PLUGIN_EXTENSION_VALS).get(ManualOpsSource.KEY_OPTIONS));
    }

    /**
     * 关键词行没有候选项来源 —— {@code component} 信封上<b>不得</b>出现 {@code appendVals}。
     *
     * <p>反向对照：若把 {@code optionsSource} 提到
     * {@link com.qlangtech.tis.plugin.ontology.workshop.widget.component.FilterComponent} 上
     * （即让关键词也带来源），本用例会变红。
     */
    @Test
    public void testNonSelectRowHasNoAppendVals() throws Exception {
        JSONObject component = componentEnvelope(descriptor.getItemJson(widget(row("keyword_only", "关键词"))), 0);
        Assert.assertFalse("关键词行不该有 appendVals", component.containsKey(AttrValMap.PLUGIN_APPEND_VALS));
        Assert.assertNull("关键词行的信封里不该有 optionsSource",
                component.getJSONObject(AttrValMap.PLUGIN_EXTENSION_VALS).get("optionsSource"));
    }

    // ===================================================================
    //  降级：解析失败 / 空配置都不能让整个序列化失败
    // ===================================================================

    /**
     * 对象集来源解析失败（变量不存在 / 无运行期上下文）-> 该行 {@code appendVals.options} 是
     * <b>空数组</b>，整份序列化<b>不抛</b>。
     *
     * <p>两条要求缺一不可：
     * <ul>
     *   <li>不抛 —— 该端点由前端在<em>每次</em>编辑后调用，一次异常会让配置面板红一片；</li>
     *   <li>空数组仍在 —— 键存在才有「解析过了、确实没有」这个事实。缺键会让前端无法区分
     *       「还没算」与「算了没结果」，从而无限补请求。</li>
     * </ul>
     */
    @Test
    public void testObjectSetFailureDegradesToEmpty() throws Exception {
        ObjectSetOpsSource broken = new ObjectSetOpsSource();
        broken.sourceObjectSetVar = "not_exist_object_set_var";
        broken.labelProperty = "report_date";
        broken.valueProperty = "report_id";

        JSONObject item = descriptor.getItemJson(widget(row("department", "部门", multiSelect(broken))));

        JSONArray options = optionsOf(optionsSourceEnvelope(item, 0));
        Assert.assertTrue("解析失败必须降级成空数组，而不是冒泡", options.isEmpty());
    }

    /**
     * {@code filters == null}（新建 Widget 时的合法中间态）-> 不抛，且 {@code vals.filters}
     * 缺席或为空数组。
     *
     * <p>前端拿到空列表会走「响应里没有可用的 filters」分支，按空候选处理，不覆盖已有候选。
     */
    @Test
    public void testEmptyFiltersStillSerializes() throws Exception {
        FilterListWidget widget = new FilterListWidget();
        widget.id = "w-empty";
        widget.objectSetVar = "daily_report_object_set";
        widget.filters = null;

        JSONObject item = descriptor.getItemJson(widget);
        Assert.assertNotNull("空 filters 也要能序列化出信封", item);

        Object rawFilters = item.getJSONObject(AttrValMap.PLUGIN_EXTENSION_VALS).get(FilterListWidget.KEY_FILTERS);
        Assert.assertTrue("filters 为 null 时不能凭空造出内容，实际：" + rawFilters,
                rawFilters == null || ((JSONArray) rawFilters).isEmpty());
    }

    /**
     * 多行按行下发、互不串味。
     *
     * <p>前端按 {@code property} 配对（{@code rows.find(r => r.property === filter.property)}），
     * 所以「按位置取」这种写法在行序变化时会串数据。本用例把两行候选设成<b>不同长度</b>，
     * 位置错配立刻可见。
     */
    @Test
    public void testEachRowCarriesItsOwnOptions() throws Exception {
        JSONObject item = descriptor.getItemJson(widget(
                row("department", "部门", multiSelect(manual(option("dept_a", "部门A")))),
                row("channel", "渠道", multiSelect(manual(option("app", "App"), option("web", "Web"))))));

        Assert.assertEquals("第 1 行", 1, optionsOf(optionsSourceEnvelope(item, 0)).size());
        Assert.assertEquals("第 2 行", 2, optionsOf(optionsSourceEnvelope(item, 1)).size());
        Assert.assertEquals(FilterItemConfig.class.getName(), filters(item).getJSONObject(0).getString("impl"));
        // 行键是 property（FilterItemConfig.identityValue）：前端按它配对候选，
        // 丢了这一项就只能按位置取，行序一变就串数据
        Assert.assertEquals("department", filters(item).getJSONObject(0).getString("identityName"));
    }

    // ===================================================================
    //  端到端：真实前端 POST 体 -> httpProcess -> bizresult
    // ===================================================================

    /**
     * 走完 {@code httpProcess} 的整条链路：前端表单投影 → {@code AttrValMap} 解析 →
     * {@code DescribableJSON} 再序列化 → {@code bizresult.vals.filters}。
     *
     * <h3>为什么要测这一条</h3>
     * 上面几个用例直接调 {@code getItemJson(instance)}，绕过了「请求体怎么被解析成实例」这段。
     * 而这一段有两处静默失效风险，且都不报错：
     * <ol>
     *   <li>字段白名单（{@code createRelevantFieldsContext} 的第二个参数）漏掉 {@code filters}，
     *       解析出的 widget 的 {@code filters} 就是 {@code null}，响应一片空；</li>
     *   <li>POST 线格式认错（叶子要 {@code {_primaryVal}}、多态要 {@code {descVal:{impl,vals}}}、
     *       type-13 的 {@code descVal.vals} 是<b>按标识做键的 map</b>），要么直接抛
     *       {@code IllegalStateException}，要么取到默认值。</li>
     * </ol>
     *
     * <h3>为什么用手写请求体而不是 {@code buildPostJsonBody()}</h3>
     * {@code buildPostJsonBody()} 是给 AI Agent 步骤用的（{@code BasicStepExecutor}），
     * 它把 type-13 字段摊成 <b>JSONArray</b> 塞进 {@code _primaryVal}，
     * 而 {@code asRootFormVals()} 要求根层每个值都是 {@code JSONObject} —— 直接抛
     * {@code type must be a object}。真实前端的 {@code Item.project()} 给出的是
     * {@code {descVal:{impl, vals:{<identity>: {impl, vals}}}}}，本用例按后者构造。
     */
    @Test
    public void testHttpProcessRoundTripsFrontendPostBody() throws Exception {
        pluginContext.setPostContent(frontendPostBody(widget(row(
                "department", "部门", multiSelect(manual(option("dept_a", "部门A")))))));

        IControlMsgHandler msgHandler = Mockito.mock(IControlMsgHandler.class);
        Mockito.doReturn(FilterListWidget.HTTP_METHOD_GET_OPTIONS)
                .when(msgHandler).getString("type");
        AtomicReference<Object> bizResult = new AtomicReference<>();
        // httpProcess 调的是 2 参重载（IMessageHandler 的 default 方法），它再转 3 参。
        // Mockito 默认把 default 方法也桩掉，所以这里必须桩 2 参这一层。
        Mockito.doAnswer(inv -> {
            bizResult.set(inv.getArgument(1));
            return null;
        }).when(msgHandler).setBizResult(ArgumentMatchers.any(), ArgumentMatchers.any());

        descriptor.httpProcess(msgHandler, pluginContext, new DefaultContext());

        Assert.assertNotNull("httpProcess 必须把序列化结果塞进 bizresult", bizResult.get());
        JSONObject item = (JSONObject) bizResult.get();
        // 白名单过了：filters 被解析回实例，而不是 null
        JSONArray options = optionsOf(optionsSourceEnvelope(item, 0));
        Assert.assertEquals("POST 上来的候选项要原样回到响应里", 1, options.size());
        Assert.assertEquals("dept_a", options.getJSONObject(0).getString(Option.KEY_VALUE));
        Assert.assertEquals("前端按 property 配对候选，行键不能丢",
                "department", filters(item).getJSONObject(0).getString("identityName"));
    }

    // ===================================================================
    //  路径取值 —— 故意写成「路径写错就断言失败 / NPE」
    // ===================================================================

    /** {@code vals.filters[i].vals.component.vals.optionsSource} */
    private static JSONObject optionsSourceEnvelope(JSONObject item, int rowIndex) {
        return componentEnvelope(item, rowIndex).getJSONObject(AttrValMap.PLUGIN_EXTENSION_VALS)
                .getJSONObject("optionsSource");
    }

    /** {@code vals.filters[i].vals.component} */
    private static JSONObject componentEnvelope(JSONObject item, int rowIndex) {
        JSONObject row = filters(item).getJSONObject(rowIndex);
        Objects.requireNonNull(row, "第 " + rowIndex + " 行不存在");
        return row.getJSONObject(AttrValMap.PLUGIN_EXTENSION_VALS).getJSONObject("component");
    }

    /** {@code vals.filters} */
    private static JSONArray filters(JSONObject item) {
        JSONArray filters = item.getJSONObject(AttrValMap.PLUGIN_EXTENSION_VALS)
                .getJSONArray(FilterListWidget.KEY_FILTERS);
        Assert.assertNotNull("vals.filters 必须存在且是数组（前端 toFilterItems 只接受数组）", filters);
        return filters;
    }

    /** {@code ...optionsSource.appendVals.options} */
    private static JSONArray optionsOf(JSONObject sourceEnvelope) {
        JSONObject appendVals = sourceEnvelope.getJSONObject(AttrValMap.PLUGIN_APPEND_VALS);
        Assert.assertNotNull("optionsSource 信封缺 appendVals —— 候选没有随 item 下发", appendVals);
        JSONArray options = appendVals.getJSONArray(Descriptor.KEY_OPTIONS);
        Assert.assertNotNull("appendVals 缺 " + Descriptor.KEY_OPTIONS + " 键", options);
        return options;
    }

    // ===================================================================
    //  构造辅助
    // ===================================================================

    private static FilterListWidget widget(FilterItemConfig... rows) {
        FilterListWidget widget = new FilterListWidget();
        widget.id = "w-filter-list";
        widget.title = "过滤器";
        widget.objectSetVar = "daily_report_object_set";
        widget.filters = new ArrayList<>(List.of(rows));
        return widget;
    }

    private static FilterItemConfig row(String property, String label) {
        return row(property, label, new KeywordComponent());
    }

    private static FilterItemConfig row(String property, String label,
                                        com.qlangtech.tis.plugin.ontology.workshop.widget.component.FilterComponent component) {
        FilterItemConfig row = new FilterItemConfig();
        row.property = property;
        row.label = label;
        row.component = component;
        return row;
    }

    private static MultiSelectComponent multiSelect(ManualOpsSource source) {
        MultiSelectComponent component = new MultiSelectComponent();
        component.optionsSource = source;
        return component;
    }

    private static MultiSelectComponent multiSelect(ObjectSetOpsSource source) {
        MultiSelectComponent component = new MultiSelectComponent();
        component.optionsSource = source;
        return component;
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

    // ===================================================================
    //  POST 线格式构造（对齐前端 Item.project()）
    // ===================================================================

    /**
     * 前端 {@code Item.project()} 的输出形状 —— {@code httpProcess} 真正吃到的东西。
     *
     * <pre>
     * { impl, vals: {
     *     id:           { _primaryVal }                                  // 叶子
     *     objectSetVar: { _primaryVal }                                  // 叶子
     *     filters: { descVal: { impl, vals: {                            // type-13
     *         "department": { impl, vals: {                              //   key = 行标识
     *             property:  { _primaryVal },
     *             label:     { _primaryVal },
     *             component: { descVal: { impl, vals: { optionsSource: { descVal: {
     *                 impl, vals: { options: { descVal: { impl, vals: {   // options 也是 type-13
     *                     "dept_a": { impl, vals: { val: {_primaryVal}, label: {_primaryVal} } }
     *                 }}}}}}}}}
     *     }}}}}
     * </pre>
     *
     * <p>与 {@code buildPostJsonBody()} 的差别正是「type-13 是 map 不是 array」——
     * 后者产出的数组会被 {@code asRootFormVals()} 直接拒掉。
     */
    private static JSONObject frontendPostBody(FilterListWidget widget) {
        JSONObject rowVals = new JSONObject();
        rowVals.put("property", primary(widget.filters.get(0).property));
        rowVals.put("label", primary(widget.filters.get(0).label));

        JSONObject optionVals = new JSONObject();
        WidgetOptionConfig option = ((ManualOpsSource) ((MultiSelectComponent) widget.filters.get(0).component).optionsSource)
                .options.get(0);
        optionVals.put("val", primary(option.val));
        optionVals.put("label", primary(option.label));

        JSONObject optionValsMap = new JSONObject();
        optionValsMap.put(option.val, describable(WidgetOptionConfig.class, optionVals));

        JSONObject sourceVals = new JSONObject();
        sourceVals.put(ManualOpsSource.KEY_OPTIONS,
                descValField(describable(ManualOpsSource.class, optionValsMap)));

        JSONObject componentVals = new JSONObject();
        componentVals.put("optionsSource",
                descValField(describable(ManualOpsSource.class, sourceVals)));

        rowVals.put("component", descValField(describable(MultiSelectComponent.class, componentVals)));

        JSONObject filtersValsMap = new JSONObject();
        filtersValsMap.put(widget.filters.get(0).property,
                describable(FilterItemConfig.class, rowVals));

        JSONObject vals = new JSONObject();
        vals.put("id", primary(widget.id));
        vals.put("title", primary(widget.title));
        vals.put("objectSetVar", primary(widget.objectSetVar));
        vals.put(FilterListWidget.KEY_FILTERS,
                descValField(describable(FilterItemConfig.class, filtersValsMap)));

        JSONObject body = new JSONObject();
        body.put(AttrValMap.PLUGIN_EXTENSION_IMPL, FilterListWidget.class.getName());
        body.put(AttrValMap.PLUGIN_EXTENSION_VALS, vals);
        return body;
    }

    /** 叶子值：{@code {_primaryVal: v}} */
    private static JSONObject primary(Object value) {
        JSONObject wrapper = new JSONObject();
        wrapper.put(Descriptor.KEY_primaryVal, value);
        return wrapper;
    }

    /** 描述符值：{@code {impl, vals}}（还不带外层 descVal） */
    private static JSONObject describable(Class<?> impl, JSONObject vals) {
        JSONObject inner = new JSONObject();
        inner.put(AttrValMap.PLUGIN_EXTENSION_IMPL, impl.getName());
        inner.put(AttrValMap.PLUGIN_EXTENSION_VALS, vals);
        return inner;
    }

    /** 多态 / type-13 字段的外层：{@code {descVal: {impl, vals}}} */
    private static JSONObject descValField(JSONObject describable) {
        JSONObject field = new JSONObject();
        field.put(Descriptor.KEY_DESC_VAL, describable);
        return field;
    }
}
