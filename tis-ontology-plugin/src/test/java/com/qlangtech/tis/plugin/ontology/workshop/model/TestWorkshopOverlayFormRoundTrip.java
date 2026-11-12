package com.qlangtech.tis.plugin.ontology.workshop.model;

import com.alibaba.citrus.turbine.impl.DefaultContext;
import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.plugin.ontology.workshop.TisPluginFormTestSupport;
import com.qlangtech.tis.plugin.ontology.workshop.model.config.NoneVariableBasedVisibility;
import com.qlangtech.tis.plugin.ontology.workshop.model.config.VariableBasedVisibility;
import com.qlangtech.tis.plugin.ontology.workshop.model.overlay.DrawerConfig;
import com.qlangtech.tis.plugin.ontology.workshop.model.page.PageTemplate;
import com.qlangtech.tis.plugin.ontology.workshop.model.page.PercentColumnWidth;
import com.qlangtech.tis.plugin.ontology.workshop.model.page.TwoColumnPageTemplate;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.util.AttrValMap;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.Objects;

import static com.qlangtech.tis.extension.Descriptor.KEY_DESC_VAL;
import static com.qlangtech.tis.extension.Descriptor.KEY_primaryVal;

/**
 * {@code WorkshopOverlay.variableBasedVisibility} 的 POST 线格式回归。
 *
 * <p>该字段改造为 off / 默认两个实现的 Describable 后，前端提交的形状是
 * {@code {descVal: {impl, vals}}}（嵌套多态层），与 GET 侧 {@code {impl, vals}} 互为逆变换。
 * 这条链路上有两处<b>没有编译期保护</b>的约定：
 *
 * <ol>
 *   <li>off 子类的 {@code vals} 必须能为空 —— 它是零字段实现，前端不提交任何叶子值。
 *       若给 off 加上必填字段，这里会抛校验异常，而面板上用户根本没得填。</li>
 *   <li>默认实现的两个必填字段必须能在 POST 侧还原（含枚举按常量名反序列化）——
 *       枚举写成大写常量名后，老值 {@code equals} 会匹配不上。</li>
 * </ol>
 *
 * <p>需要 TIS 运行期（{@code TIS.get().getDescriptor(impl)}）；环境不具备时整条跳过，
 * 与 {@code TestWorkshopPageFormRoundTrip} 同口径。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/11/11
 */
public class TestWorkshopOverlayFormRoundTrip {

    private static final String OVERLAY_ID = "9b1c2a8e-9b3d-4c5e-8f70-1a2b3c4d5e6f";

    /** off 形态：零字段子类，vals 里不提交任何叶子值 */
    @Test
    public void offVisibilitySurvivesRoundTrip() throws Exception {
        Assume.assumeTrue("当前环境未初始化 TIS，跳过 overlay 线格式检查", tisOverlayDescriptorReachable());

        JSONObject postItem = overlayPostItem(
                describable(NoneVariableBasedVisibility.class.getName(), new JSONObject()));

        WorkshopOverlay overlay = WorkshopFormParser.parse(IControlMsgHandler.namedContext("workshop"),
                new DefaultContext(), postItem, "overlay[0]", WorkshopOverlay.class);

        Assert.assertEquals("off 实现必须能被实例化出来（零字段子类不应有必填校验）",
                NoneVariableBasedVisibility.class, overlay.variableBasedVisibility.getClass());
        assertTemplateParsed(overlay);
    }

    /** 启用形态：variableId + expression + compareValue 全部经 POST 线格式还原 */
    @Test
    public void enabledVisibilitySurvivesRoundTrip() throws Exception {
        Assume.assumeTrue("当前环境未初始化 TIS，跳过 overlay 线格式检查", tisOverlayDescriptorReachable());

        JSONObject visVals = new JSONObject();
        visVals.put("variableId", primary("region"));
        visVals.put("expression", primary("greaterThan"));
        visVals.put("compareValue", primary("100"));
        JSONObject postItem = overlayPostItem(
                describable(VariableBasedVisibility.class.getName(), visVals));

        WorkshopOverlay overlay = WorkshopFormParser.parse(IControlMsgHandler.namedContext("workshop"),
                new DefaultContext(), postItem, "overlay[0]", WorkshopOverlay.class);

        Assert.assertEquals(VariableBasedVisibility.class, overlay.variableBasedVisibility.getClass());
        VariableBasedVisibility visibility = (VariableBasedVisibility) overlay.variableBasedVisibility;
        Assert.assertEquals("region", visibility.variableId);
        Assert.assertEquals("expression 必须按枚举常量名反序列化 —— 老值 greaterThan 读不回就是丢配置",
                VariableBasedVisibility.VisibilityExpression.greaterThan, visibility.expression);
        Assert.assertEquals("100", visibility.compareValue);
        assertTemplateParsed(overlay);
    }

    /**
     * 覆盖层也走页面那套 {@code template}（多态 Describable）线格式 ——
     * 与页面唯一不同的地方是它<b>没有</b> {@code displayName} 字段，
     * 所以这里专门钉「非默认栏宽能不能读回」，别让模板静默退化成两栏 30%。
     */
    @Test
    public void overlayTemplateResourceAndFallback() {
        JSONObject resource = TisPluginFormTestSupport.formResource(WorkshopOverlay.class.getName());
        Assert.assertNotNull("WorkshopOverlay.json 应存在", resource);
        Assert.assertFalse("type 字段已删除（与 typeConfig.impl 重复），json 里不能再有它",
                resource.containsKey("type"));
        Assert.assertFalse("title 字段已删除（与 name 重复），json 里不能再有它",
                resource.containsKey("title"));
        Assert.assertEquals("WorkshopOverlay.json 的 template.dftVal 必须等于模板子类的 getDisplayName()",
                "Two_Column_Page_Template",
                resource.getJSONObject("template").getString("dftVal"));
    }

    /** 存量 overlay XML 没有 template 元素时，readResolve 必须补默认模板（否则画布无法渲染栅格） */
    @Test
    public void readResolveFillsMissingTemplate() throws Exception {
        WorkshopOverlay overlay = new WorkshopOverlay();
        overlay.template = null;

        Method readResolve = WorkshopOverlay.class.getDeclaredMethod("readResolve");
        readResolve.setAccessible(true);
        readResolve.invoke(overlay);

        Assert.assertNotNull("readResolve 缺失字段时应补默认模板", overlay.template);
        Assert.assertEquals(TwoColumnPageTemplate.class, overlay.template.getClass());
        Assert.assertEquals("默认左栏宽度应为 30%（与前端 null 分支一致）",
                Integer.valueOf(30),
                ((PercentColumnWidth) ((TwoColumnPageTemplate) overlay.template).leftColumnWidth).percentage);
    }

    private static void assertTemplateParsed(WorkshopOverlay overlay) {
        Assert.assertNotNull("template 必须被解析成插件实例", overlay.template);
        Assert.assertEquals("template.impl 必须是 TwoColumnPageTemplate",
                TwoColumnPageTemplate.class, overlay.template.getClass());
        PageTemplate template = overlay.template;
        Assert.assertEquals("percentage 必须按 _primaryVal 反序列化（用 45% 才能区分于默认 30%）",
                Integer.valueOf(45),
                ((PercentColumnWidth) ((TwoColumnPageTemplate) template).leftColumnWidth).percentage);
    }

    // ===================================================================
    //  helpers
    // ===================================================================

    /** 与前端 {@code item.project()} 同构的 overlay POST item（只填必填字段） */
    private static JSONObject overlayPostItem(JSONObject visibility) {
        JSONObject typeConfigVals = new JSONObject();
        typeConfigVals.put("width", primary(480));
        typeConfigVals.put("placement", primary("right"));

        JSONObject vals = new JSONObject();
        vals.put("id", primary(OVERLAY_ID));
        vals.put("name", primary("详情浮层"));
        vals.put("template", describable(TwoColumnPageTemplate.class.getName(),
                valsOf("leftColumnWidth", describable(PercentColumnWidth.class.getName(),
                        valsOf("percentage", primary(45))))));
        vals.put("typeConfig", describable(DrawerConfig.class.getName(), typeConfigVals));
        vals.put("variableBasedVisibility", visibility);

        JSONObject item = new JSONObject();
        item.put(AttrValMap.PLUGIN_EXTENSION_IMPL, WorkshopOverlay.class.getName());
        item.put(AttrValMap.PLUGIN_EXTENSION_VALS, vals);
        return item;
    }

    private static JSONObject valsOf(String key, Object value) {
        JSONObject vals = new JSONObject();
        vals.put(key, value);
        return vals;
    }

    /** POST 侧嵌套 Describable 包一层 {@code {descVal: {impl, vals}}} */
    private static JSONObject describable(String impl, JSONObject vals) {
        JSONObject descVal = new JSONObject();
        descVal.put(AttrValMap.PLUGIN_EXTENSION_IMPL, impl);
        descVal.put(AttrValMap.PLUGIN_EXTENSION_VALS, vals);
        JSONObject wrapped = new JSONObject();
        wrapped.put(KEY_DESC_VAL, descVal);
        return wrapped;
    }

    /** POST 侧叶子必须包一层 {@code _primaryVal}（裸值会被 parseAttrValMap 拒绝） */
    private static JSONObject primary(Object value) {
        JSONObject wrapped = new JSONObject();
        wrapped.put(KEY_primaryVal, value);
        return wrapped;
    }

    /** descriptor 扫描是否可用（惰性列表必须在 try 里物化，否则异常会漏成失败而非跳过） */
    private static boolean tisOverlayDescriptorReachable() {
        try {
            TIS tis = TIS.get();
            return tis != null && !Objects.requireNonNull(tis.getDescriptorList(WorkshopOverlay.class)).isEmpty();
        } catch (Throwable t) {
            return false;
        }
    }
}
