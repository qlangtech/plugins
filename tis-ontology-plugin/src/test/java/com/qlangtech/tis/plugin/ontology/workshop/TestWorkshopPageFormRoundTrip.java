package com.qlangtech.tis.plugin.ontology.workshop;

import com.alibaba.citrus.turbine.impl.DefaultContext;
import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopFormParser;
import com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopPage;
import com.qlangtech.tis.plugin.ontology.workshop.model.page.PageTemplate;
import com.qlangtech.tis.plugin.ontology.workshop.model.page.PercentColumnWidth;
import com.qlangtech.tis.plugin.ontology.workshop.model.page.TwoColumnPageTemplate;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import com.qlangtech.tis.util.AttrValMap;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import static com.qlangtech.tis.extension.Descriptor.KEY_DESC_VAL;
import static com.qlangtech.tis.extension.Descriptor.KEY_primaryVal;

/**
 * 页面线格式与「解析 → 校验 → newInstance」链路的回归测试。
 *
 * <h3>为什么需要它</h3>
 * 页面以前是服务端<b>手写反序列化</b>的（逐字段 {@code getString} + {@code new WorkshopPage()}），
 * 与其他实体走的不是同一条路。现在改为前端提交 {@code {impl, vals}} 的 POST item，
 * 服务端经 {@link WorkshopFormParser}（{@code validateWithScope(STRICT)} + {@code newInstance()}）
 * 实例化 —— 这条链路上有三处<b>没有编译期保护</b>的约定，全部由本类钉住：
 *
 * <ol>
 *   <li><b>POST 包装形状</b> —— 叶子值必须包 {@code {_primaryVal: v}}；裸值会让
 *       {@code AttrVals.parseAttrValMap} 直接抛错。</li>
 *   <li><b>嵌套多态线格式</b> —— {@code template} 是 {@link PageTemplate}（Describable），
 *       POST 侧必须包 {@code {descVal: {impl, vals}}}，而不是裸字符串，
 *       且 {@code impl} 是插件类的<b>全限定名</b>（{@code Descriptor#getId()} 恒等于
 *       {@code clazz.getName()}）。写成旧枚举名 / kebab 短名都会在
 *       {@code TIS.get().getDescriptor(impl)} 处静默取到 null 再抛异常。</li>
 *   <li><b>descriptor 的类型匹配</b> —— {@code newInstance()} 必须产出 {@link WorkshopPage}；
 *       impl 写错时 {@code WorkshopFormParser} 的 {@code expectedType} 检查会报错，
 *       而不是把一个别的实体静默落盘。</li>
 * </ol>
 *
 * <p>需要 TIS 运行期（{@code TIS.get().getDescriptor(impl)}），环境不具备时整条跳过；
 * 用 {@code -DargLine="--add-opens java.base/java.lang=ALL-UNNAMED"} 可以本地跑起来。
 */
public class TestWorkshopPageFormRoundTrip {

    /**
     * JSON 里 template 的 {@code dftVal} 必须逐字等于某个模板子类的 {@code getDisplayName()}。
     *
     * <p>多态 Describable 字段的默认项由「dftVal == 子类 getDisplayName()」匹配
     * （见 {@code PropertyType} 的默认项解析）；写成旧枚举常量名（{@code BLANK}）时
     * 没有任何子类能匹配上，表单在新增模式下会**没有预选实现**，且不报错。
     */
    @Test
    public void templateResourceDefaultMatchesImplDisplayName() {
        JSONObject resource = TisPluginFormTestSupport.formResource(WorkshopPage.class.getName());
        Assert.assertNotNull("WorkshopPage.json 应存在", resource);
        Assert.assertEquals("WorkshopPage.json 的 template.dftVal 必须等于模板子类的 getDisplayName()",
                "Two_Column_Page_Template",
                resource.getJSONObject("template").getString("dftVal"));
    }

    /**
     * 缺省模板的形状 —— {@link PageTemplate#createDefault()} 是页面缺失 template 时的唯一兜底，
     * 前端在「还没选模板」时按两栏 30% 渲染（{@code extractPageLayoutStyle} 的 null 分支），
     * 两处必须一致，否则存档前后页面栅格会跳一下。
     */
    @Test
    public void defaultTemplateIsTwoColumnThirtyPercent() {
        PageTemplate template = PageTemplate.createDefault();
        Assert.assertEquals("缺省模板必须是两栏布局",
                TwoColumnPageTemplate.class, template.getClass());
        TwoColumnPageTemplate twoColumn = (TwoColumnPageTemplate) template;
        Assert.assertEquals("缺省左栏宽度必须是百分比实现",
                PercentColumnWidth.class, twoColumn.leftColumnWidth.getClass());
        Assert.assertEquals("缺省左栏宽度必须是 30%（与前端 null 分支一致）",
                Integer.valueOf(30), ((PercentColumnWidth) twoColumn.leftColumnWidth).percentage);
        Assert.assertNotSame("每次调用都必须返回新实例，避免调用方改动共享默认值",
                template, PageTemplate.createDefault());
    }

    /**
     * 前端投影出的页面 POST item 能被 {@link WorkshopFormParser} 还原成完整实例。
     *
     * <p>样本刻意取<b>非默认</b>模板（{@code TwoColumnPageTemplate} + 45% 左栏）：默认值
     * 若是两栏 30%，字段整个丢了也会「看起来正确」—— 用不同的栏宽才能同时钉住
     * 「template 本身解析成功」和「嵌套的 leftColumnWidth 也解析成功」。
     */
    @Test
    public void pagePostItemRoundTrip() throws Exception {
        Assume.assumeTrue("当前环境未初始化 TIS（JDK17 下通常还缺 "
                        + "--add-opens java.base/java.lang=ALL-UNNAMED），跳过页面线格式检查",
                tisPageDescriptorReachable());

        String pageId = "6f1c2a8e-9b3d-4c5e-8f70-1a2b3c4d5e6f";
        JSONObject postItem = pagePostItem(pageId, "销售看板", 3);

        IControlMsgHandler msgHandler = IControlMsgHandler.namedContext("workshop");
        WorkshopPage page = WorkshopFormParser.parse(msgHandler, new DefaultContext(),
                postItem, "page[0]", WorkshopPage.class);

        Assert.assertEquals(pageId, page.id);
        Assert.assertEquals("销售看板", page.name);
        Assert.assertNotNull("template 必须被解析成插件实例", page.template);
        Assert.assertEquals("template.impl 必须是 TwoColumnPageTemplate",
                TwoColumnPageTemplate.class, page.template.getClass());
        TwoColumnPageTemplate twoColumn = (TwoColumnPageTemplate) page.template;
        Assert.assertNotNull("leftColumnWidth 是嵌套多态字段，必须一并解析", twoColumn.leftColumnWidth);
        Assert.assertEquals(PercentColumnWidth.class.getName(),
                twoColumn.leftColumnWidth.getDescriptor().getId());
        Assert.assertEquals("percentage 必须按 _primaryVal 反序列化",
                Integer.valueOf(45), ((PercentColumnWidth) twoColumn.leftColumnWidth).percentage);
        Assert.assertEquals("sortOrder 先取表单值（随后会被信封覆盖）", Integer.valueOf(3), page.sortOrder);
    }

    /**
     * {@code routeId}（页面 URL 标识）的往返：POST 能解析进来、GET 能下发出去。
     *
     * <p>两处都是<b>静默失效</b>型：POST 侧解析不到只是字段恒为 null（页面照样能存），
     * GET 侧 {@code pageToJSON} 是手写 JSON，漏写 {@code routeId} 不报错、前端永远拿
     * {@code undefined}，URL 恒回落成 UUID 段 —— 用户会以为「填了 URL 标识但链接没变」。
     */
    @Test
    public void routeIdRoundTrip() throws Exception {
        Assume.assumeTrue("当前环境未初始化 TIS（JDK17 下通常还缺 "
                        + "--add-opens java.base/java.lang=ALL-UNNAMED），跳过 routeId 线格式检查",
                tisPageDescriptorReachable());

        String pageId = "7f2d3b9f-ac4e-4d6f-9a81-2b3c4d5e6f70";
        String routeId = "case-detail";
        JSONObject postItem = pagePostItem(pageId, "案件详情", 1);
        postItem.getJSONObject(AttrValMap.PLUGIN_EXTENSION_VALS)
                .put("routeId", primary(routeId));

        IControlMsgHandler msgHandler = IControlMsgHandler.namedContext("workshop");
        WorkshopPage page = WorkshopFormParser.parse(msgHandler, new DefaultContext(),
                postItem, "page[0]", WorkshopPage.class);

        Assert.assertEquals("routeId 必须按 _primaryVal 反序列化", routeId, page.routeId);

        // GET 侧：pageToJSON 是手写 JSON，不经过 DescribableJSON，因此单独钉一次
        JSONObject pageJson = pageToJSON(page);
        Assert.assertTrue("pageToJSON 必须下发 routeId，否则前端永远拿不到"
                + "（漏写不报错，表现为「填了 URL 标识但链接仍是 UUID」）",
                pageJson.containsKey("routeId"));
        Assert.assertEquals(routeId, pageJson.getString("routeId"));
    }

    /**
     * {@code routeId} 是可选的：空值必须能存、GET 也照常下发（值为 null）。
     *
     * <p>{@link Validator#identity} 的规则带 {@code guardNotBlank=true}，空值直接通过 ——
     * 这条约束一旦被改成 {@code require}，「没填 URL 标识的页面」就整批存不进去了。
     */
    @Test
    public void emptyRouteIdIsAllowed() throws Exception {
        Assume.assumeTrue("当前环境未初始化 TIS（JDK17 下通常还缺 "
                        + "--add-opens java.base/java.lang=ALL-UNNAMED），跳过 routeId 线格式检查",
                tisPageDescriptorReachable());

        JSONObject postItem = pagePostItem("8a3e4c0a-bd5f-4e70-ab92-3c4d5e6f7081", "无标识页", 2);
        // 空串（用户填了又删空，前端提交的就是 ""，不是缺键）
        postItem.getJSONObject(AttrValMap.PLUGIN_EXTENSION_VALS).put("routeId", primary(""));

        IControlMsgHandler msgHandler = IControlMsgHandler.namedContext("workshop");
        WorkshopPage page = WorkshopFormParser.parse(msgHandler, new DefaultContext(),
                postItem, "page[0]", WorkshopPage.class);

        Assert.assertNull("空 routeId 是合法的（可选字段），不应被校验拦下", page.routeId);
        Assert.assertNull("GET 侧照常下发该键，值为 null",
                pageToJSON(page).getString("routeId"));
    }

    /**
     * {@code routeId} 的字符集校验来自 {@link Validator#identity}：不能以数字开头。
     *
     * <p>这条并非可有可无 —— 以数字开头的段在 URL 里无法与路径层级 / 版本号等数字段
     * 区分，是 {@code ValidatorCommons.PATTERN_FORBID_START_WITH_NUMBER} 存在的理由。
     * 直接调校验器（不经过表单）即可，无需 TIS 运行期。
     */
    @Test
    public void routeIdMustNotStartWithNumber() {
        Assert.assertFalse("以数字开头的 routeId 必须校验失败",
                Validator.identity.validate(fieldErrorHandler(), new DefaultContext(),
                        "routeId", "1case-detail"));
        Assert.assertTrue("允许以字母开头（含连字符）",
                Validator.identity.validate(fieldErrorHandler(), new DefaultContext(),
                        "routeId", "case-detail"));
        Assert.assertTrue("空值直接通过（guardNotBlank）",
                Validator.identity.validate(fieldErrorHandler(), new DefaultContext(),
                        "routeId", ""));
    }

    /**
     * {@code WorkshopPage.json} 里必须给 {@code routeId} 配上 label / help。
     *
     * <p>缺条目时表单照常渲染，只是这个字段既没有中文标签也没有说明 ——
     * 「页面 ID（可选）」是用户理解「这是写进 URL 的」的唯一途径。
     */
    @Test
    public void routeIdHasFormResourceEntry() {
        JSONObject resource = TisPluginFormTestSupport.formResource(WorkshopPage.class.getName());
        Assert.assertNotNull("WorkshopPage.json 应存在", resource);
        JSONObject routeIdMeta = resource.getJSONObject("routeId");
        Assert.assertNotNull("WorkshopPage.json 缺少 routeId 条目", routeIdMeta);
        Assert.assertNotNull("routeId 的 label 不能为空", routeIdMeta.getString("label"));
        Assert.assertNotNull("routeId 的 help 不能为空（要说明「写进 URL，改了旧链接会失效」）",
                routeIdMeta.getString("help"));
    }

    // ===================================================================
    //  routeId 唯一性（同批 / 并上磁盘存量）
    // ===================================================================

    /**
     * 同一批页面里 routeId 重号必须抛错 —— 这是快照路径（{@code module-save}）的校验点。
     *
     * <p>重号的后果不是「报错」而是「同一个链接指向两页」：前端按首个匹配渲染，
     * 另一页<b>永远打不开</b>，且没有任何日志。
     */
    @Test
    public void duplicateRouteIdsInSameBatchAreRejected() {
        assertDuplicateRejected(page("page-a", "home"), page("page-b", "home"));
    }

    /**
     * 多个页面都没有 routeId 是<b>合法</b>的（可选字段），不能当成重号。
     *
     * <p>空值在唯一性检查里必须先 {@code trimToNull} 再跳过：写成「空串也算一个值」
     * 会让「所有人都没填 URL 标识」的模块整批保存失败。
     */
    @Test
    public void multipleEmptyRouteIdsAreAllowed() {
        // 不抛即通过
        invokeAssertUniqueRouteIds(List.of(page("page-a", null), page("page-b", ""), page("page-c", "  ")));
        invokeAssertUniqueRouteIds(List.of(page("page-a", "   "), page("page-b", "home")));
    }

    /**
     * routeId 前后的空白不构成差异：`" home"` 与 `"home"` 是同一个 URL 段。
     *
     * <p>校验按 {@code trimToNull} 归一，所以这里必须被判成重号 —— 否则用户填了
     * `" home "` 就能绕过唯一性，而它在地址栏里和 `home` 完全等价。
     */
    @Test
    public void routeIdUniquenessIgnoresSurroundingWhitespace() {
        assertDuplicateRejected(page("page-a", "home"), page("page-b", " home "));
    }


    // ===================================================================
    //  helpers
    // ===================================================================

    /** 与前端 {@code item.project()} 同构的页面 POST item */
    private static JSONObject pagePostItem(String id, String name, int sortOrder) {
        JSONObject vals = new JSONObject();
        vals.put("id", primary(id));
        vals.put("name", primary(name));
        vals.put("displayName", primary(name));
        vals.put("template", describable(TwoColumnPageTemplate.class.getName(),
                valsOf("leftColumnWidth", describable(PercentColumnWidth.class.getName(),
                        valsOf("percentage", primary(45))))));
        vals.put("sortOrder", primary(sortOrder));

        JSONObject item = new JSONObject();
        item.put(AttrValMap.PLUGIN_EXTENSION_IMPL, WorkshopPage.class.getName());
        item.put(AttrValMap.PLUGIN_EXTENSION_VALS, vals);
        // 信封字段：不是 @FormField，与 vals 平级
        item.put("sortOrder", sortOrder);
        return item;
    }

    /**
     * POST 侧的嵌套 Describable 包一层 {@code {descVal: {impl, vals}}} ——
     * 与 GET 侧 `{impl, vals}` 是**互为逆变换**的两套形状（名字都叫 impl/vals 但层数不同）。
     */
    private static JSONObject describable(String impl, JSONObject vals) {
        JSONObject descVal = new JSONObject();
        descVal.put(AttrValMap.PLUGIN_EXTENSION_IMPL, impl);
        descVal.put(AttrValMap.PLUGIN_EXTENSION_VALS, vals);
        JSONObject wrapped = new JSONObject();
        wrapped.put(KEY_DESC_VAL, descVal);
        return wrapped;
    }

    private static JSONObject valsOf(String key, Object value) {
        JSONObject vals = new JSONObject();
        vals.put(key, value);
        return vals;
    }

    /** POST 侧叶子必须包一层 {@code _primaryVal}（裸值会被 parseAttrValMap 拒绝） */
    private static JSONObject primary(Object value) {
        JSONObject wrapped = new JSONObject();
        wrapped.put(KEY_primaryVal, value);
        return wrapped;
    }

    /** 页面 POST item 的 vals 段（直接改它就能加字段） */
    private static WorkshopPage page(String id, String routeId) {
        WorkshopPage page = new WorkshopPage();
        page.id = id;
        page.name = id;
        page.routeId = routeId;
        return page;
    }

    /**
     * 调 {@code WorkshopModule#assertUniqueRouteIds}。
     *
     * <p>它是 {@code private static}（内部校验，产品上只经 {@code parsePages} /
     * {@code doApplyModule} 进入），这里走反射直连：走 {@code parsePages} 需要 JSON POST 体、
     * 走 {@code doApplyModule} 需要磁盘存量，都会把「一个纯校验逻辑」的用例变成集成测试。
     */
    private static void invokeAssertUniqueRouteIds(Collection<WorkshopPage> pages) {
        try {
            Method m = uniqueRouteIdAsserter();
            m.invoke(null, pages);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            throw new AssertionError("assertUniqueRouteIds 抛了非运行时异常", cause);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("无法调用 assertUniqueRouteIds（方法被改名/移走了？）", e);
        }
    }

    private static void assertDuplicateRejected(WorkshopPage... pages) {
        try {
            invokeAssertUniqueRouteIds(Arrays.asList(pages));
            Assert.fail("routeId 重号必须抛错：同一个 URL 段指向两页时，前端按首个匹配渲染，"
                    + "另一页永远打不开且没有任何日志");
        } catch (IllegalStateException expected) {
            Assert.assertTrue("错误信息里应带上重号的 routeId，实际是：" + expected.getMessage(),
                    expected.getMessage().contains("duplicate page routeId"));
        }
    }

    /** {@code assertUniqueRouteIds} 是 {@code WorkshopModule.DefaultDescriptor} 的私有静态方法 */
    private static Method uniqueRouteIdAsserter() throws NoSuchMethodException {
        Method m = WorkshopModule.DefaultDescriptor.class
                .getDeclaredMethod("assertUniqueRouteIds", Collection.class);
        m.setAccessible(true);
        return m;
    }

    /** {@code pageToJSON} 同样是 {@code DefaultDescriptor} 的私有静态方法 */
    private static JSONObject pageToJSON(WorkshopPage page) {
        try {
            Method m = WorkshopModule.DefaultDescriptor.class
                    .getDeclaredMethod("pageToJSON", WorkshopPage.class);
            m.setAccessible(true);
            return (JSONObject) m.invoke(null, page);
        } catch (InvocationTargetException e) {
            throw new AssertionError("pageToJSON 抛异常", e.getCause());
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("无法调用 pageToJSON（方法被改名/移走了？）", e);
        }
    }

    /**
     * 直接调 {@link Validator} 用的字段错误处理器。
     *
     * <p>用 Mockito 而不是手写实现：{@link IFieldErrorHandler} 是给 Web 层用的胖接口
     * （还有 {@code validateBizLogic} / {@code getPipelineValidator} 等与校验器无关的方法），
     * 手写实现会让「我只关心 addFieldError 有没有被调」这件事被一堆空方法淹没。
     */
    private static IFieldErrorHandler fieldErrorHandler() {
        return Mockito.mock(IFieldErrorHandler.class);
    }

    /** descriptor 扫描是否可用（惰性列表必须在 try 里物化，否则异常会漏成失败而非跳过） */
    private static boolean tisPageDescriptorReachable() {
        try {
            TIS tis = TIS.get();
            return tis != null && !Objects.requireNonNull(tis.getDescriptorList(WorkshopPage.class)).isEmpty();
        } catch (Throwable t) {
            return false;
        }
    }
}
