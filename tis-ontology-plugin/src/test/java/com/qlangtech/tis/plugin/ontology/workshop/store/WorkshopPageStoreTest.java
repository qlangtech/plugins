package com.qlangtech.tis.plugin.ontology.workshop.store;

import com.qlangtech.tis.plugin.ontology.workshop.WorkshopModule;
import com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopPage;
import com.qlangtech.tis.plugin.ontology.workshop.model.page.PageTemplate;
import com.qlangtech.tis.util.IPluginContext;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * {@link WorkshopPageStore} 落盘链路的端到端回归测试。
 *
 * <p>覆盖 editor「保存」实际使用的快照写入语义：
 * <ol>
 *   <li>整对象覆盖写 —— 文件路径由 {@code page.id} 决定，字段能原样读回；</li>
 *   <li>{@code replaceAll} 下 {@code sortOrder} 按快照数组顺序归一化；</li>
 *   <li>差集删除 —— 快照里没有的页面文件会被删掉（删除页面后保存的核心行为）；</li>
 *   <li>目录按模块隔离：同一 domain 下不同模块的页面互不可见；</li>
 *   <li>增量 {@code save} 不做归一化（显式 {@code sortOrder} 原样落盘），
 *       删除不存在的页面返回 {@code false}。</li>
 * </ol>
 */
public class WorkshopPageStoreTest {

    private static final String ONTOLOGY_DOMAIN_ID = "test_workshop_page_domain";
    private static final String MODULE_NAME = "test_workshop_page_module";
    private static final String OTHER_MODULE_NAME = "test_workshop_page_module_other";

    @After
    public void cleanUp() {
        FileUtils.deleteQuietly(WorkshopModule.getWorkshopDir(ONTOLOGY_DOMAIN_ID, MODULE_NAME));
        FileUtils.deleteQuietly(WorkshopModule.getWorkshopDir(ONTOLOGY_DOMAIN_ID, OTHER_MODULE_NAME));
    }

    @Test
    public void testReplaceAllRoundTripAndDiffDelete() {
        IPluginContext pluginContext = Mockito.mock(IPluginContext.class);
        WorkshopPageStore store = WorkshopPageStore.create(ONTOLOGY_DOMAIN_ID, MODULE_NAME);

        WorkshopPage first = page("11111111-1111-1111-1111-111111111111", "页面 1", PageTemplate.BLANK);
        WorkshopPage second = page("22222222-2222-2222-2222-222222222222", "页面 2", PageTemplate.GRID);

        Set<String> removed = store.replaceAll(pluginContext, Optional.empty(), List.of(first, second));
        Assert.assertTrue("首次保存不应删除任何页面", removed.isEmpty());

        File firstFile = store.getPageFile(first.id);
        Assert.assertTrue("页面应落在 " + firstFile.getAbsolutePath(), firstFile.isFile());

        List<WorkshopPage> loaded = store.listAll();
        Assert.assertEquals(2, loaded.size());
        Assert.assertEquals("页面 1", loaded.get(0).name);
        Assert.assertEquals(PageTemplate.BLANK, loaded.get(0).template);
        Assert.assertEquals(Integer.valueOf(0), loaded.get(0).sortOrder);
        Assert.assertEquals("页面 2", loaded.get(1).name);
        Assert.assertEquals(PageTemplate.GRID, loaded.get(1).template);
        Assert.assertEquals(Integer.valueOf(1), loaded.get(1).sortOrder);

        // 第二轮快照只保留 second，且改了名字：first 的文件应被删除，second 原地覆盖
        second.name = "页面 2 改名";
        removed = store.replaceAll(pluginContext, Optional.empty(), List.of(second));
        Assert.assertEquals(Set.of(first.id), removed);
        Assert.assertFalse("快照中已不存在的页面文件应被删除", firstFile.isFile());

        loaded = store.listAll();
        Assert.assertEquals(1, loaded.size());
        Assert.assertEquals(second.id, loaded.get(0).id);
        Assert.assertEquals("页面 2 改名", loaded.get(0).name);
        Assert.assertEquals(Integer.valueOf(0), loaded.get(0).sortOrder);
    }

    @Test
    public void testPagesAreModuleScoped() {
        IPluginContext pluginContext = Mockito.mock(IPluginContext.class);
        WorkshopPage page = page("33333333-3333-3333-3333-333333333333", "只属于模块 A", PageTemplate.DETAILS);
        WorkshopPageStore.create(ONTOLOGY_DOMAIN_ID, MODULE_NAME)
                .replaceAll(pluginContext, Optional.empty(), List.of(page));

        Assert.assertTrue(WorkshopPageStore.create(ONTOLOGY_DOMAIN_ID, MODULE_NAME)
                .listAll().size() == 1);
        Assert.assertTrue("另一个模块不应看到本模块的页面",
                WorkshopPageStore.create(ONTOLOGY_DOMAIN_ID, OTHER_MODULE_NAME).listAll().isEmpty());
    }

    @Test
    public void testSingleSaveKeepsExplicitSortOrder() {
        IPluginContext pluginContext = Mockito.mock(IPluginContext.class);
        WorkshopPageStore store = WorkshopPageStore.create(ONTOLOGY_DOMAIN_ID, MODULE_NAME);

        // 增量路径（module-apply）不再归一化，sortOrder 由前端信封给出，必须原样落盘
        WorkshopPage page = page("44444444-4444-4444-4444-444444444444", "显式序号", PageTemplate.BLANK);
        page.sortOrder = 7;
        store.save(pluginContext, Optional.empty(), page);

        Assert.assertEquals(Integer.valueOf(7), store.load(page.id).sortOrder);
    }

    @Test
    public void testDeleteMissingPageReturnsFalse() {
        IPluginContext pluginContext = Mockito.mock(IPluginContext.class);
        WorkshopPageStore store = WorkshopPageStore.create(ONTOLOGY_DOMAIN_ID, MODULE_NAME);

        Assert.assertFalse("删除不存在的页面应返回 false",
                store.delete(pluginContext, Optional.empty(), "55555555-5555-5555-5555-555555555555"));
    }

    private static WorkshopPage page(String id, String name, PageTemplate template) {
        WorkshopPage page = new WorkshopPage();
        page.id = id;
        page.name = name;
        page.displayName = name;
        page.template = template;
        return page;
    }
}
