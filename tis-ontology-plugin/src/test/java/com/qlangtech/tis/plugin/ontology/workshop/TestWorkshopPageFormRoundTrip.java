package com.qlangtech.tis.plugin.ontology.workshop;

import com.alibaba.citrus.turbine.impl.DefaultContext;
import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopFormParser;
import com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopPage;
import com.qlangtech.tis.plugin.ontology.workshop.model.page.PageTemplate;
import com.qlangtech.tis.plugin.ontology.workshop.model.page.PercentColumnWidth;
import com.qlangtech.tis.plugin.ontology.workshop.model.page.TwoColumnPageTemplate;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.util.AttrValMap;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

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
