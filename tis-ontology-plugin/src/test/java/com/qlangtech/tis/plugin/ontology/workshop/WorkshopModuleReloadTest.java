package com.qlangtech.tis.plugin.ontology.workshop;

import com.qlangtech.tis.TIS;
import com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopCptParentId;
import com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopPage;
import com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopSection;
import com.qlangtech.tis.plugin.ontology.workshop.model.page.PageTemplate;
import com.qlangtech.tis.plugin.ontology.workshop.store.WorkshopPageStore;
import com.qlangtech.tis.plugin.ontology.workshop.store.WorkshopSectionStore;
import com.qlangtech.tis.plugin.ontology.workshop.store.WorkshopWidgetStore;
import com.qlangtech.tis.plugin.ontology.workshop.widget.WorkshopWidget;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.TextInputWidget;
import com.qlangtech.tis.util.IPluginContext;
import com.qlangtech.tis.util.XStream2;
import com.thoughtworks.xstream.io.xml.XppDriver;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Workshop 模块子实体回填（{@code ensureWorkshopData} / {@code readResolve} / 写侧失效）的回归测试。
 *
 * <p>历史故障：page / section / widget 实例被 {@code PluginStore} 缓存复用，旧 {@code loadLayout}
 * 每次读取都对共享单例 {@code addSection/addWidget}，列表随访问次数线性增长直至 OOM。
 * 本类钉住修复后的三条不变量：
 * <ol>
 *   <li><b>幂等</b>：同一模块实例反复回填，{@code page.sections} / {@code section.widgets}
 *       恒等于落盘实体数（旧实现会变成 10、20...）。</li>
 *   <li><b>写后重建</b>：store 写成功递增 generation，下一次回填读到新数据，且不会因重建而重复追加。</li>
 *   <li><b>整体赋值</b>：删除实体后重建得到空列表（append + clear 的竞态写不出这个结果）。</li>
 * </ol>
 *
 * <p>domain / module 每个测试方法单独生成，避免命中进程级 {@code Memoizer} 缓存里的旧路径。
 */
public class WorkshopModuleReloadTest {

    private static final IPluginContext PLUGIN_CONTEXT = Mockito.mock(IPluginContext.class);

    private String domain;
    private String moduleName;

    @Before
    public void setUp() {
        // 每次用全新目录：PluginStore 以文件绝对路径为键做进程级缓存，复用名字会串味
        String suffix = UUID.randomUUID().toString().replace("-", "");
        this.domain = "ws_reload_domain_" + suffix;
        this.moduleName = "ws_reload_module_" + suffix;
    }

    @After
    public void cleanUp() {
        FileUtils.deleteQuietly(WorkshopModule.getWorkshopDir(domain, moduleName));
    }

    @Test
    public void testEnsureIsIdempotentAndRebuildDoesNotAppend() {
        String pageId = UUID.randomUUID().toString();
        String sectionId = UUID.randomUUID().toString();
        persistLayout(pageId, sectionId);

        WorkshopModule module = newModule();
        module.ensureWorkshopData(domain, moduleName);
        assertLayout(module, pageId, 1, sectionId, 1);

        // 纯读取路径：ensure 的 loaded 短路
        for (int i = 0; i < 10; i++) {
            module.ensureWorkshopData(domain, moduleName);
        }
        assertLayout(module, pageId, 1, sectionId, 1);

        // 强制重建路径：每次 invalidate 都会真正执行一遍 loadLayout。
        // 旧实现在这里会对共享单例重复 append，10 次后 sections 变成 10 个。
        for (int i = 0; i < 10; i++) {
            WorkshopModule.invalidateWorkshopData(domain, moduleName);
            module.ensureWorkshopData(domain, moduleName);
            assertLayout(module, pageId, 1, sectionId, 1);
        }
    }

    @Test
    public void testSaveInvalidatesAndReassignsWholeList() {
        String pageId = UUID.randomUUID().toString();
        String firstSectionId = UUID.randomUUID().toString();
        persistLayout(pageId, firstSectionId);

        WorkshopModule module = newModule();
        module.ensureWorkshopData(domain, moduleName);
        assertLayout(module, pageId, 1, firstSectionId, 1);

        // store.save 内部已经递增 generation，下一次 ensure 必须重建
        String secondSectionId = UUID.randomUUID().toString();
        WorkshopSectionStore.create(domain, moduleName).save(PLUGIN_CONTEXT, Optional.empty(),
                section(secondSectionId, "分区 2", 1, pageId));

        module.ensureWorkshopData(domain, moduleName);
        assertLayout(module, pageId, 2, firstSectionId, 1);

        // 重建后再读：不能因为「重建」本身又追加一份
        module.ensureWorkshopData(domain, moduleName);
        Assert.assertEquals(2, page(module, pageId).sections.size());

        // 删除后重建 = 整体赋值成空列表，而不是在旧列表上继续累加
        WorkshopSectionStore.create(domain, moduleName)
                .delete(PLUGIN_CONTEXT, Optional.empty(), firstSectionId);
        WorkshopSectionStore.create(domain, moduleName)
                .delete(PLUGIN_CONTEXT, Optional.empty(), secondSectionId);
        module.ensureWorkshopData(domain, moduleName);
        Assert.assertTrue("section 全部删除后，页面下不应残留任何 section",
                page(module, pageId).sections.isEmpty());
    }

    @Test
    public void testConcurrentRebuildKeepsListBounded() throws Exception {
        String pageId = UUID.randomUUID().toString();
        String firstSectionId = UUID.randomUUID().toString();
        String secondSectionId = UUID.randomUUID().toString();
        persistLayout(pageId, firstSectionId);
        WorkshopSectionStore.create(domain, moduleName).save(PLUGIN_CONTEXT, Optional.empty(),
                section(secondSectionId, "分区 2", 1, pageId));

        WorkshopModule first = newModule();
        WorkshopModule second = newModule();
        first.ensureWorkshopData(domain, moduleName);
        second.ensureWorkshopData(domain, moduleName);

        // 两个模块实例从同一个 PluginStore 拿到的是同一批 page/section 实例，
        // 并发重建时只能看到「完整旧快照」或「完整新快照」，不能交错增长
        ExecutorService pool = Executors.newFixedThreadPool(6);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(6);
        AtomicBoolean failed = new AtomicBoolean(false);
        AtomicInteger maxObserved = new AtomicInteger();
        for (int i = 0; i < 6; i++) {
            final WorkshopModule target = (i % 2 == 0) ? first : second;
            pool.submit(() -> {
                try {
                    start.await();
                    for (int round = 0; round < 30; round++) {
                        WorkshopModule.invalidateWorkshopData(domain, moduleName);
                        target.ensureWorkshopData(domain, moduleName);
                        int size = page(target, pageId).sections.size();
                        maxObserved.accumulateAndGet(size, Math::max);
                        if (size > 2) {
                            failed.set(true);
                        }
                    }
                } catch (Throwable t) {
                    failed.set(true);
                    t.printStackTrace();
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        Assert.assertTrue("并发重建超时", done.await(60, TimeUnit.SECONDS));
        pool.shutdownNow();

        Assert.assertFalse("并发重建出现了重复 append", failed.get());
        Assert.assertTrue("列表长度不应超过落盘实体数，实际观察到 " + maxObserved.get(),
                maxObserved.get() <= 2);
        Assert.assertEquals(2, page(first, pageId).sections.size());
        Assert.assertEquals(2, page(second, pageId).sections.size());
    }

    @Test
    public void testLegacyXmlWithoutDomainDoesNotBackfill() {
        // 老模块 XML 没有 ontologyDomain：readResolve 必须安静跳过，
        // 交给 WorkshopModule.load(domain, name) 携带权威参数兜底
        WorkshopModule legacy = new WorkshopModule();
        legacy.name = moduleName;

        XStream2 xstream = new XStream2(new XppDriver());
        String xml = xstream.toXML(legacy);
        WorkshopModule restored = (WorkshopModule) xstream.fromXML(xml);

        Assert.assertNotNull(restored);
        Assert.assertEquals(moduleName, restored.name);
        // transient 字段不会跑字段初始化，反序列化后就是 null；
        // 无 domain 时 readResolve 必须什么都不做，后续由 load(domain, name) 回填
        Assert.assertTrue("没有 domain 时不应猜测目录并回填",
                restored.pages == null || restored.pages.isEmpty());
    }

    /**
     * 有 domain 时 unmarshal 即回填（readResolve 被 XStream 调用）。
     * <p>
     * 依赖 TIS 运行期（子实体 store 走 {@code TIS.getPluginStore}），环境不具备时跳过。
     */
    @Test
    public void testUnmarshalBackfillsWhenDomainPresent() throws Exception {
        Assume.assumeTrue("当前环境 TIS 未初始化，跳过 readResolve 回填检查", tisReachable());

        String pageId = UUID.randomUUID().toString();
        String sectionId = UUID.randomUUID().toString();
        persistLayout(pageId, sectionId);

        WorkshopModule prototype = newModule();
        setOntologyDomain(prototype, domain);

        XStream2 xstream = new XStream2(new XppDriver());
        String xml = xstream.toXML(prototype);
        // 防止其它测试的字段初始化让断言的命题空转：XML 里必须带着 domain
        Assert.assertTrue("序列化结果应包含 ontologyDomain", xml.contains(domain));

        WorkshopModule restored = (WorkshopModule) xstream.fromXML(xml);
        Assert.assertEquals("反序列化后应已回填页面，无需调用 load()",
                1, restored.pages.size());
        assertLayout(restored, pageId, 1, sectionId, 1);
    }

    // ===================================================================
    //  helpers
    // ===================================================================

    private WorkshopModule newModule() {
        WorkshopModule module = new WorkshopModule();
        module.name = moduleName;
        return module;
    }

    private void persistLayout(String pageId, String sectionId) {
        WorkshopPageStore.create(domain, moduleName).save(PLUGIN_CONTEXT, Optional.empty(), page(pageId));
        WorkshopSectionStore.create(domain, moduleName).save(PLUGIN_CONTEXT, Optional.empty(),
                section(sectionId, "分区 1", 0, pageId));
        WorkshopWidgetStore.create(domain, moduleName).save(PLUGIN_CONTEXT, Optional.empty(),
                widget("Widget 1", sectionId));
    }

    private static void assertLayout(WorkshopModule module, String pageId,
                                     int expectedSections, String sectionId, int expectedWidgets) {
        WorkshopPage page = page(module, pageId);
        Assert.assertEquals("page.sections 数量应等于落盘 section 数",
                expectedSections, page.sections.size());
        WorkshopSection section = findSection(page, sectionId);
        Assert.assertNotNull("应能找到 section:" + sectionId, section);
        Assert.assertEquals("section.widgets 数量应等于落盘 widget 数",
                expectedWidgets, section.widgets.size());
    }

    private static WorkshopPage page(WorkshopModule module, String pageId) {
        for (WorkshopPage page : module.pages) {
            if (pageId.equals(page.id)) {
                return page;
            }
        }
        throw new AssertionError("page not found:" + pageId);
    }

    private static WorkshopSection findSection(WorkshopPage page, String sectionId) {
        for (WorkshopSection section : page.sections) {
            if (sectionId.equals(section.id)) {
                return section;
            }
        }
        return null;
    }

    private static WorkshopPage page(String id) {
        WorkshopPage page = new WorkshopPage();
        page.id = id;
        page.name = "页面";
        page.displayName = "页面";
        page.template = PageTemplate.BLANK;
        return page;
    }

    private static WorkshopSection section(String id, String name, int sortOrder, String pageId) {
        WorkshopSection section = new WorkshopSection();
        section.id = id;
        section.name = name;
        section.sortOrder = sortOrder;
        section.parent = WorkshopCptParentId.pageParent(pageId);
        return section;
    }

    private static WorkshopWidget widget(String id, String sectionId) {
        TextInputWidget widget = new TextInputWidget();
        widget.id = id;
        widget.title = id;
        widget.sectionId = sectionId;
        return widget;
    }

    private static void setOntologyDomain(WorkshopModule module, String value) throws Exception {
        Field field = WorkshopModule.class.getDeclaredField("ontologyDomain");
        field.setAccessible(true);
        field.set(module, value);
    }

    private static boolean tisReachable() {
        try {
            TIS tis = TIS.get();
            return tis != null;
        } catch (Throwable t) {
            return false;
        }
    }
}
