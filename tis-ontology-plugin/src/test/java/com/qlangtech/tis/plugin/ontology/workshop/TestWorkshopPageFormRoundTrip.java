package com.qlangtech.tis.plugin.ontology.workshop;

import com.alibaba.citrus.turbine.impl.DefaultContext;
import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopPage;
import com.qlangtech.tis.plugin.ontology.workshop.model.page.PageTemplate;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.util.AttrValMap;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.util.Objects;

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
 *   <li><b>枚举线格式</b> —— {@code template} 是 {@link PageTemplate}，框架走
 *       {@code Enum.valueOf}（{@code PageTemplate} 没有实现 {@code IEnumTypeSer} 兜大小写），
 *       因此线上必须是常量名 {@code TWO_COLUMN}，而不是前端实体上的小写 {@code two_column}。
 *       前端在投影时做大写归一（{@code entity-form.model.ts#toFormFieldValue}）。</li>
 *   <li><b>descriptor 的类型匹配</b> —— {@code newInstance()} 必须产出 {@link WorkshopPage}；
 *       impl 写错时 {@code WorkshopFormParser} 的 {@code expectedType} 检查会报错，
 *       而不是把一个别的实体静默落盘。</li>
 * </ol>
 *
 * <p>需要 TIS 运行期（{@code TIS.get().getDescriptor(impl)}），环境不具备时整条跳过；
 * 用 {@code -DargLine="--add-opens java.base/java.lang=ALL-UNNAMED"} 可以本地跑起来。
 */
public class TestWorkshopPageFormRoundTrip {

    /** JSON 里 template 的默认值必须与 Java 字段初始化 / 枚举名一致 */
    @Test
    public void templateResourceDefaultMatchesEnumConstant() {
        JSONObject resource = TisPluginFormTestSupport.formResource(WorkshopPage.class.getName());
        Assert.assertNotNull("WorkshopPage.json 应存在", resource);
        Assert.assertEquals("WorkshopPage.json 的 template.dftVal 必须是枚举常量名（前端按它保留默认值）",
                PageTemplate.BLANK.name(),
                resource.getJSONObject("template").getString("dftVal"));

        // 前端实体用小写拼 CSS 类名（page-template-two_column），表单/线格式用常量名 ——
        // 两者只差大小写，写错时不会有任何编译或运行期报错
        Assert.assertEquals("TWO_COLUMN", PageTemplate.TWO_COLUMN.name());
        Assert.assertEquals("two_column", PageTemplate.TWO_COLUMN.name().toLowerCase());
    }

    /**
     * 前端投影出的页面 POST item 能被 {@link WorkshopFormParser} 还原成完整实例。
     *
     * <p>样本刻意取<b>非默认</b>模板（{@code TWO_COLUMN}）：默认值 {@code BLANK} 即使字段丢了
     * 也会因为 Java 字段初始化而「看起来正确」，抓不住线格式错误。
     */
    @Test
    public void pagePostItemRoundTrip() {
        Assume.assumeTrue("当前环境未初始化 TIS（JDK17 下通常还缺 "
                        + "--add-opens java.base/java.lang=ALL-UNNAMED），跳过页面线格式检查",
                tisPageDescriptorReachable());

        String pageId = "6f1c2a8e-9b3d-4c5e-8f70-1a2b3c4d5e6f";
        JSONObject postItem = pagePostItem(pageId, "销售看板", "TWO_COLUMN", 3);

        IControlMsgHandler msgHandler = IControlMsgHandler.namedContext("workshop");
        WorkshopPage page = WorkshopFormParser.parse(msgHandler, new DefaultContext(),
                postItem, "page[0]", WorkshopPage.class);

        Assert.assertEquals(pageId, page.id);
        Assert.assertEquals("销售看板", page.name);
        Assert.assertEquals("template 必须按枚举常量名反序列化", PageTemplate.TWO_COLUMN, page.template);
        Assert.assertEquals("sortOrder 先取表单值（随后会被信封覆盖）", Integer.valueOf(3), page.sortOrder);
    }

    // ===================================================================
    //  helpers
    // ===================================================================

    /** 与前端 {@code item.project()} 同构的页面 POST item */
    private static JSONObject pagePostItem(String id, String name, String template, int sortOrder) {
        JSONObject vals = new JSONObject();
        vals.put("id", primary(id));
        vals.put("name", primary(name));
        vals.put("displayName", primary(name));
        vals.put("template", primary(template));
        vals.put("sortOrder", primary(sortOrder));

        JSONObject item = new JSONObject();
        item.put(AttrValMap.PLUGIN_EXTENSION_IMPL, WorkshopPage.class.getName());
        item.put(AttrValMap.PLUGIN_EXTENSION_VALS, vals);
        // 信封字段：不是 @FormField，与 vals 平级
        item.put("sortOrder", sortOrder);
        return item;
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
