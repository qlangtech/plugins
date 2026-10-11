package com.qlangtech.tis.plugin.ontology.workshop.widget.impl;

import com.alibaba.fastjson.JSONObject;
import com.koubei.web.tag.pager.Pager;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.ontology.workshop.TisPluginFormTestSupport;
import com.qlangtech.tis.plugin.ontology.workshop.widget.DefaultPagenator;
import com.qlangtech.tis.plugin.ontology.workshop.widget.NoPagenator;
import com.qlangtech.tis.plugin.ontology.workshop.widget.Pagenator;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * 分页策略（{@link Pagenator} 插件族）的契约测试。
 *
 * <p>三块互相独立的东西：
 * <ol>
 *   <li><b>纯逻辑</b>（{@link ObjectTableWidget.DescriptorImpl#parsePage} /
 *       {@code clampPage}、各子类的 {@code resolvePageSize()}）—— 不触达 TIS 插件扫描，
 *       无需 {@code --add-opens}，在默认 surefire 配置下就能真跑；</li>
 *   <li><b>结构契约</b>（字段确实是多态、旧字段已删、子类恰好两个且有一个是 off）；</li>
 *   <li><b>跨语言约定</b>（{@code ObjectTableWidget.json} 的 {@code dftVal} 逐字等于关闭子类的
 *       {@code displayName}）—— 这条没有任何编译期保护，写错的表现是「新建的 Widget 默认
 *       落到了开启分页的子类上」，一行日志都没有。</li>
 * </ol>
 *
 * <p>负向对照（想确认这些断言不是恒真）：把 {@code ObjectTableWidget.json} 的
 * {@code pagenator.dftVal} 改成 {@code "OFF"}，{@link #hostFormResourceDefaultsToTheOffSubclass()}
 * 应当变红；把 {@link ObjectTableWidget#pagenator} 改回 {@code Integer}，
 * {@link #pagenatorFieldIsPolymorphic()} 应当变红；打断 {@code Pager.getCurPage()} 的夹取分支，
 * {@link #testClampPageRoundsTotalPageUp()} 应当变红（证明夹取真的穿到了 {@code Pager}）。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/9
 */
public class TestObjectTableWidgetPagenator {

    /** 分页器族所在的包 —— sezpoz 索引按包前缀圈定扫描范围 */
    private static final String PAGENATOR_PKG = "com.qlangtech.tis.plugin.ontology.workshop.widget";

    // ===================================================================
    // 1. 页码解析
    // ===================================================================

    @Test
    public void testParsePageDefaultsToFirstPage() {
        Assert.assertEquals(1, ObjectTableWidget.DescriptorImpl.parsePage(null));
        Assert.assertEquals(1, ObjectTableWidget.DescriptorImpl.parsePage(""));
        Assert.assertEquals(1, ObjectTableWidget.DescriptorImpl.parsePage("   "));
    }

    @Test
    public void testParsePageAcceptsPositiveIntegers() {
        Assert.assertEquals(1, ObjectTableWidget.DescriptorImpl.parsePage("1"));
        Assert.assertEquals(3, ObjectTableWidget.DescriptorImpl.parsePage("3"));
        // 前后空白宽容处理：query string 里正常不会带，但带了不该炸
        Assert.assertEquals(7, ObjectTableWidget.DescriptorImpl.parsePage(" 7 "));
    }

    /**
     * 非法页码必须抛。这是本协议里**唯一**「算错了也照样出数据」的地方：把 {@code page=0} 或
     * {@code page=-1} 静默当成第 1 页，用户以为在第 3 页、其实一直在看第 1 页，从界面上看不出来。
     */
    @Test
    public void testIllegalPageThrows() {
        for (String illegal : new String[]{"abc", "0", "-1", "1.5", "二"}) {
            try {
                ObjectTableWidget.DescriptorImpl.parsePage(illegal);
                Assert.fail("illegal page should be rejected:" + illegal);
            } catch (IllegalArgumentException e) {
                Assert.assertTrue("异常信息里应带上原值，便于定位：" + e.getMessage(),
                        e.getMessage().contains(illegal));
            }
        }
    }

    // ===================================================================
    // 2. 页码越界夹取
    // ===================================================================

    /**
     * 每次调用都必须给一个**新的** {@link Pager}：{@code getCurPage()} 会把夹取结果写回自己的
     * {@code curPage}（有副作用），复用同一实例会让上一次的夹取结果残留到下一次。
     * 生产代码里也是每次请求 new 一个（见 {@code get_data}），这里保持一致。
     */
    private static int clamp(int page, int pageSize, long totalCount) {
        return ObjectTableWidget.DescriptorImpl.clampPage(new Pager(null), page, pageSize, totalCount);
    }

    /** totalCount=21、pageSize=20 → 2 页；写成整除会算成 1 页，第 2 页就永远翻不过去 */
    @Test
    public void testClampPageRoundsTotalPageUp() {
        Assert.assertEquals("恰好整除", 2, clamp(2, 20, 40));
        Assert.assertEquals("多出 1 条也算一页", 2, clamp(2, 20, 21));
        Assert.assertEquals("第 3 页已越过末页，夹到第 2 页", 2, clamp(3, 20, 21));
    }

    @Test
    public void testClampPageKeepsInRangePage() {
        Assert.assertEquals(1, clamp(1, 20, 100));
        Assert.assertEquals(5, clamp(5, 20, 100));
    }

    /**
     * 没有数据（以及关闭分页时后端回的 {@link ObjectTableWidget#NO_TOTAL_COUNT} 哨兵）一律落第 1 页。
     *
     * <p>哨兵 {@code -1} 必须走这条而不是被算成「负数总页数」：{@code (long) Math.min(1, 负)} 会得到负数，
     * 那是个连 {@code BasicCriteria} 都表示不了的页码。
     */
    @Test
    public void testClampPageFallsBackToFirstWhenNoData() {
        Assert.assertEquals(1, clamp(1, 20, 0));
        Assert.assertEquals(1, clamp(9, 20, 0));
        Assert.assertEquals("NO_TOTAL_COUNT 哨兵",
                1, clamp(9, 20, ObjectTableWidget.NO_TOTAL_COUNT));
    }

    /** pageSize 非法（0 / 负数）时不做除法，直接落第 1 页 —— 否则是 ArithmeticException，比夹取更难查 */
    @Test
    public void testClampPageToleratesIllegalPageSize() {
        Assert.assertEquals(1, clamp(3, 0, 100));
        Assert.assertEquals(1, clamp(3, -5, 100));
    }

    /**
     * 哨兵值必须**原样**到达响应体：关态下 {@code get_data} 报的 {@code totalCount} 是
     * {@link ObjectTableWidget#NO_TOTAL_COUNT}（-1），前端 `object-table.component.ts` 靠
     * {@code totalCount >= 0} 认出「这个总数不可用」并保留自己上一轮的值。
     *
     * <p>这条钉的是「夹取用的 Pager 和序列化出去的 Pager 是同一个」：若各建一个，哨兵很容易
     * 在其中一个上被丢掉（{@code Pager.totalCount} 默认是 0，而 0 会被前端当成「数出来是 0 条」）。
     */
    @Test
    public void testClampPageKeepsNoTotalCountSentinelOnThePager() {
        Pager pager = new Pager(null);
        Assert.assertEquals("关态页码落第 1 页",
                1, ObjectTableWidget.DescriptorImpl.clampPage(
                        pager, 9, 20, ObjectTableWidget.NO_TOTAL_COUNT));
        Assert.assertEquals("哨兵要留在 Pager 上供 PaginationResult 序列化",
                ObjectTableWidget.NO_TOTAL_COUNT, pager.getTotalCount());
    }

    // ===================================================================
    // 3. 两个子类的行为
    // ===================================================================

    @Test
    public void testNoPagenatorIsOffAndTakesOnePage() {
        NoPagenator off = new NoPagenator();
        Assert.assertFalse("关态不应开启分页", off.pagingEnabled());
        Assert.assertEquals("关态取第一页的条数",
                Pagenator.DEFAULT_PAGE_SIZE, off.resolvePageSize());
    }

    @Test
    public void testDefaultPagenatorResolvesPageSize() {
        DefaultPagenator pagenator = new DefaultPagenator();
        Assert.assertTrue(pagenator.pagingEnabled());
        Assert.assertEquals("未填时回落默认条数",
                Pagenator.DEFAULT_PAGE_SIZE, pagenator.resolvePageSize());

        pagenator.pageSize = 10;
        Assert.assertEquals(10, pagenator.resolvePageSize());

        // 表单清空会落 null（见前端 toOptionalNumber 的同类口径），也应当回落而不是 NPE
        pagenator.pageSize = null;
        Assert.assertEquals(Pagenator.DEFAULT_PAGE_SIZE, pagenator.resolvePageSize());
    }

    /** `showPageNumber` 的字段初值必须是 true：它的语义是「显示总数+页码」，不该默认把能力关掉 */
    @Test
    public void testDefaultPagenatorShowsPageNumberByDefault() {
        Assert.assertTrue(new DefaultPagenator().showPageNumber);
    }

    // ===================================================================
    // 4. 结构契约
    // ===================================================================

    /** 宿主字段必须声明为抽象基类类型（多态）；退回标量则分页策略又变回一个裸配置项 */
    @Test
    public void pagenatorFieldIsPolymorphic() throws Exception {
        Assert.assertEquals("ObjectTableWidget.pagenator 应声明为抽象基类类型（多态）",
                Pagenator.class, ObjectTableWidget.class.getField("pagenator").getType());
        Assert.assertTrue("抽象基类不应被实例化",
                java.lang.reflect.Modifier.isAbstract(Pagenator.class.getModifiers()));
    }

    /**
     * 旧的 {@code pageSize: Integer} 必须已经删干净 —— 留着它会出现两个分页配置项，
     * 且旧的那个对服务端是静默 no-op（它不在相关字段白名单里），从界面上分辨不出该信哪个。
     */
    @Test
    public void legacyPageSizeFieldIsGone() {
        for (Field field : ObjectTableWidget.class.getFields()) {
            Assert.assertNotEquals("ObjectTableWidget 不应再有裸的 pageSize 字段", "pageSize", field.getName());
        }
    }

    /**
     * 恰好两个具体子类，且其中一个的 {@code displayName} 就是 {@link Descriptor#SWITCH_OFF}。
     *
     * <p>顺带钉住「抽象基类不混进面板」：{@link Pagenator} 自己不带 {@code @TISExtension}，
     * 故它不该作为子类出现在索引里（sezpoz 不过滤抽象类，一旦有人给基类也加上注解，
     * 面板上就会多出一个无法实例化的条目）。
     */
    @Test
    public void exactlyTwoSubclassesOneOfWhichIsOff() {
        Set<Class<?>> subs = TisPluginFormTestSupport.concreteSubclassesOf(Pagenator.class, PAGENATOR_PKG);
        Set<String> names = TisPluginFormTestSupport.names(subs);
        Assert.assertEquals("Pagenator 应恰好有两个具体子类（一个开启、一个关闭），实际：" + names,
                new TreeSet<>(List.of("DefaultPagenator", "NoPagenator")), names);

        List<String> offs = new ArrayList<>();
        for (Class<?> sub : subs) {
            if (Descriptor.SWITCH_OFF.equals(TisPluginFormTestSupport.descriptorOf(sub).getDisplayName())) {
                offs.add(sub.getSimpleName());
            }
        }
        Assert.assertEquals("应恰好有一个子类的 displayName 是 SWITCH_OFF（前端按它选默认项），实际是：" + offs,
                1, offs.size());
    }

    // ===================================================================
    // 5. 表单资源（跨语言约定）
    // ===================================================================

    /**
     * {@code ObjectTableWidget.json} 的 {@code pagenator.dftVal} 必须逐字等于关闭子类的
     * {@code displayName}（{@code "off"}）。
     *
     * <p>后端按这个字符串给新 Widget 选默认子类，前端 {@code resolvePagenator} 也对它 ——
     * 写成 {@code "OFF"} / {@code "关闭"} 都不报错，只会静默落到 {@code DefaultPagenator} 上，
     * 于是「新建的表格默认不分页」这个约定无声地失效了。
     */
    @Test
    public void hostFormResourceDefaultsToTheOffSubclass() {
        JSONObject resource = TisPluginFormTestSupport.formResource(
                "com.qlangtech.tis.plugin.ontology.workshop.widget.impl.ObjectTableWidget");
        Assert.assertNotNull("ObjectTableWidget.json 应存在", resource);

        JSONObject pagenatorMeta = resource.getJSONObject(ObjectTableWidget.KEY_PAGENATOR);
        Assert.assertNotNull(ObjectTableWidget.KEY_PAGENATOR + " 在 ObjectTableWidget.json 里应有条目",
                pagenatorMeta);
        Assert.assertEquals("dftVal 必须逐字等于关闭子类的 displayName",
                TisPluginFormTestSupport.descriptorOf(NoPagenator.class).getDisplayName(),
                pagenatorMeta.getString("dftVal"));
        Assert.assertEquals("关闭子类的 displayName 应当就是框架常量 SWITCH_OFF",
                Descriptor.SWITCH_OFF, pagenatorMeta.getString("dftVal"));

        Assert.assertNull("旧的 pageSize 条目应已从 json 里删除",
                resource.getJSONObject("pageSize"));
    }

    /**
     * 零字段的关态子类<b>不该</b>有 json（写了是死文件：TIS 只在类上存在 {@code @FormField}
     * 时才去查同名资源）；有字段的开态子类<b>必须</b>有，且键集与字段集一致 ——
     * 漏一个字段表现是「那个输入框没有 help / label」，不报错。
     */
    @Test
    public void formResourcesMatchFormFields() {
        Assert.assertNull("NoPagenator 零字段，不应有表单资源",
                TisPluginFormTestSupport.formResource(NoPagenator.class.getName()));

        JSONObject resource = TisPluginFormTestSupport.formResource(DefaultPagenator.class.getName());
        Assert.assertNotNull("DefaultPagenator 有字段，应有表单资源", resource);
        Assert.assertEquals("表单资源的键集应与 @FormField 字段集一致",
                formFieldNames(DefaultPagenator.class), new TreeSet<>(resource.keySet()));
    }

    /** 类上所有 {@code @FormField} 字段的名字 */
    private static Set<String> formFieldNames(Class<?> pluginClass) {
        Set<String> names = new TreeSet<>();
        for (Field field : pluginClass.getFields()) {
            if (field.getAnnotation(FormField.class) != null) {
                names.add(field.getName());
            }
        }
        Assert.assertFalse(pluginClass.getSimpleName() + " 上应至少有一个 @FormField 字段", names.isEmpty());
        return names;
    }
}
