package com.qlangtech.tis.plugin.ontology.workshop.store;

import com.qlangtech.tis.plugin.ontology.workshop.WorkshopModule;
import com.qlangtech.tis.plugin.ontology.workshop.enums.SectionParentType;
import com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopCptParentId;
import com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopSection;
import com.qlangtech.tis.plugin.ontology.workshop.widget.WorkshopWidget;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.TextInputWidget;
import com.qlangtech.tis.util.IPluginContext;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * section / widget 独立落盘链路的回归测试。
 *
 * <p>覆盖：
 * <ol>
 *   <li>section 的归属外键（{@code parent}）与序号能原样读回；</li>
 *   <li>widget 的存量 identity（旧版可编辑名称）含中文 / 空格 / 括号时，
 *       落盘文件名经过编码，读回后 identity 仍是原值；</li>
 *   <li>存量 XML 兼容：widget 的 identity 字段已改名为 {@code id}，但 XML 元素名
 *       仍是 {@code <name>}（{@code XStream#aliasField} 注册），否则旧文件读回后 id 为 null；</li>
 *   <li>{@link AbstractWorkshopEntityStore#deleteNotIn(Set)} 按 identity（而非文件名）
 *       做差集删除 —— widget 的 identity 与文件名不同，这条最容易写错；</li>
 *   <li>增量写入语义：{@code save} 只碰目标实体的文件，兄弟文件的内容原样保留；</li>
 *   <li>identity 覆盖写入（先写新值、再删旧值）后本模块只剩一个文件，内容是新的 ——
 *       只覆盖 store 的写入顺序，产品层已不再暴露「改名」；</li>
 *   <li>删除不存在的实体返回 {@code false}，不是异常也不是误删。</li>
 * </ol>
 */
public class WorkshopLayoutStoreTest {

    private static final String ONTOLOGY_DOMAIN_ID = "test_workshop_layout_domain";
    private static final String MODULE_NAME = "test_workshop_layout_module";

    private static final String PAGE_ID = "11111111-1111-1111-1111-111111111111";
    private static final String SECTION_ID = "22222222-2222-2222-2222-222222222222";
    private static final String SECTION_ID_2 = "33333333-3333-3333-3333-333333333333";

    @After
    public void cleanUp() {
        FileUtils.deleteQuietly(WorkshopModule.getWorkshopDir(ONTOLOGY_DOMAIN_ID, MODULE_NAME));
    }

    @Test
    public void testSectionRoundTripWithParent() {
        IPluginContext pluginContext = Mockito.mock(IPluginContext.class);
        WorkshopSectionStore store = WorkshopSectionStore.create(ONTOLOGY_DOMAIN_ID, MODULE_NAME);

        WorkshopSection section = section(SECTION_ID, "分区 1", 3);
        store.save(pluginContext, Optional.empty(), section);

        Assert.assertTrue(store.getEntityFile(SECTION_ID).isFile());
        WorkshopSection loaded = store.load(SECTION_ID);
        Assert.assertNotNull(loaded);
        Assert.assertEquals("分区 1", loaded.name);
        Assert.assertEquals(Integer.valueOf(3), loaded.sortOrder);
        Assert.assertEquals(PAGE_ID, loaded.parent.parentId);
        Assert.assertEquals(SectionParentType.PAGE, loaded.parent.parentType);

        Set<String> removed = store.replaceAll(pluginContext, Optional.empty(),
                List.of(section(SECTION_ID_2, "分区 2", 0)));
        Assert.assertEquals(Set.of(SECTION_ID), removed);
        Assert.assertFalse(store.getEntityFile(SECTION_ID).isFile());
        Assert.assertEquals(1, store.listAll().size());
    }

    @Test
    public void testLegacyWidgetIdIsEncodedAsStorageKey() {
        IPluginContext pluginContext = Mockito.mock(IPluginContext.class);
        WorkshopWidgetStore store = WorkshopWidgetStore.create(ONTOLOGY_DOMAIN_ID, MODULE_NAME);

        TextInputWidget widget = widget("Widget 1 (副本)", SECTION_ID);
        store.save(pluginContext, Optional.empty(), widget);

        // 文件名不是原始 id —— 括号、空格直接拼进路径不安全
        String expectedFileName = AbstractWorkshopEntityStore.encodeIdentity(widget.id)
                + com.qlangtech.tis.extension.impl.XmlFile.KEY_XML_DOT_EXTENSION;
        File widgetFile = new File(store.getStoreDir(), expectedFileName);
        Assert.assertTrue("widget 应以编码后的文件名落盘: " + widgetFile.getAbsolutePath(), widgetFile.isFile());

        List<WorkshopWidget> loaded = store.listAll();
        Assert.assertEquals(1, loaded.size());
        Assert.assertEquals("Widget 1 (副本)", loaded.get(0).identityValue());
        Assert.assertEquals(SECTION_ID, loaded.get(0).sectionId);

        // 差集删除的入参是 identity，编码在 store 内部完成
        TextInputWidget kept = widget("Widget 2", SECTION_ID_2);
        store.save(pluginContext, Optional.empty(), kept);
        Set<String> removed = store.deleteNotIn(Set.of(kept.identityValue()));
        Assert.assertEquals(Set.of("Widget 1 (副本)"), removed);
        Assert.assertFalse(widgetFile.isFile());
        Assert.assertTrue(store.getEntityFile("Widget 2").isFile());
    }

    @Test
    public void testSaveOnlyTouchesTargetEntity() throws Exception {
        IPluginContext pluginContext = Mockito.mock(IPluginContext.class);
        WorkshopWidgetStore store = WorkshopWidgetStore.create(ONTOLOGY_DOMAIN_ID, MODULE_NAME);

        TextInputWidget first = widget("Widget A", SECTION_ID);
        TextInputWidget second = widget("Widget B", SECTION_ID);
        store.save(pluginContext, Optional.empty(), first);
        store.save(pluginContext, Optional.empty(), second);

        File firstFile = store.getEntityFile(first.id);
        String firstContentBefore = FileUtils.readFileToString(firstFile, StandardCharsets.UTF_8);

        // 只保存第二个实体：增量写入不应该重写（更不应该重编号）第一个
        second.title = "Widget B 改过";
        store.save(pluginContext, Optional.empty(), second);

        Assert.assertEquals("未提交的兄弟实体不应被重写",
                firstContentBefore, FileUtils.readFileToString(firstFile, StandardCharsets.UTF_8));
        Assert.assertEquals("Widget B 改过", store.load(second.id).title);
        Assert.assertEquals(2, store.listAll().size());
    }

    /**
     * 存量 XML 兼容：字段改名为 {@code id} 后，XML 元素名必须仍是旧的 {@code <name>}。
     *
     * <p>兼容依赖 {@link WorkshopWidgetStore} 静态块里的 {@code aliasField} 注册 ——
     * 不能用 {@code @XStreamAlias}：TIS 的 {@code XStream2} 没开注解自动处理，注解不生效。
     * 这里既验证新写出的形态，也手工落一份旧格式文件验证读回路径。
     */
    @Test
    public void testLegacyXmlElementNameRemainsReadable() throws Exception {
        IPluginContext pluginContext = Mockito.mock(IPluginContext.class);
        WorkshopWidgetStore store = WorkshopWidgetStore.create(ONTOLOGY_DOMAIN_ID, MODULE_NAME);

        TextInputWidget widget = widget("Widget 1", SECTION_ID);
        store.save(pluginContext, Optional.empty(), widget);

        File widgetFile = store.getEntityFile(widget.id);
        String xml = FileUtils.readFileToString(widgetFile, StandardCharsets.UTF_8);
        Assert.assertTrue("存量格式要求元素名仍是 <name>，实际内容：\n" + xml,
                xml.contains("<name>"));
        Assert.assertFalse("字段改名后不得写出 <id> 元素，实际内容：\n" + xml,
                xml.contains("<id>"));

        // 以真实落盘结构（KeyedPluginStore → plugins → widget）手工构造一份「旧版本」文件：
        // identity 元素名是 <name>，文件名沿用旧 identity 的编码。
        // 这里替换的是写出的 XML 里的值，能确保结构/属性与历史文件一致。
        String legacyId = "Legacy Widget";
        File legacyFile = store.getEntityFile(legacyId);
        String legacyXml = xml.replace("<name>Widget 1</name>", "<name>" + legacyId + "</name>");
        Assert.assertTrue("测试构造的旧格式文件应包含 <name> 元素", legacyXml.contains("<name>" + legacyId + "</name>"));
        FileUtils.writeStringToFile(legacyFile, legacyXml, StandardCharsets.UTF_8);

        WorkshopWidget loaded = store.load(legacyId);
        Assert.assertNotNull("旧格式 widget XML 应能读回", loaded);
        Assert.assertEquals(legacyId, loaded.identityValue());
        Assert.assertEquals(SECTION_ID, loaded.sectionId);
    }

    @Test
    public void testIdentityReplacementUpsertsNewThenDeletesOld() {
        IPluginContext pluginContext = Mockito.mock(IPluginContext.class);
        WorkshopWidgetStore store = WorkshopWidgetStore.create(ONTOLOGY_DOMAIN_ID, MODULE_NAME);

        TextInputWidget original = widget("Widget 旧名", SECTION_ID);
        store.save(pluginContext, Optional.empty(), original);

        // store 层只负责写入顺序：先 upsert 新 identity、再删除旧 identity。
        // 产品层已不再暴露改名（name 只读 + 客户端 UUID），这里覆盖的是 store 机制本身。
        TextInputWidget replaced = widget("Widget 新名", SECTION_ID);
        replaced.title = "改名后的标题";
        store.save(pluginContext, Optional.empty(), replaced);
        Assert.assertTrue("旧 identity 实体应被删除",
                store.delete(pluginContext, Optional.empty(), original.id));

        Assert.assertFalse(store.getEntityFile(original.id).isFile());
        Assert.assertTrue(store.getEntityFile(replaced.id).isFile());

        List<WorkshopWidget> loaded = store.listAll();
        Assert.assertEquals("替换 identity 之后本模块只应剩一个文件", 1, loaded.size());
        Assert.assertEquals("Widget 新名", loaded.get(0).identityValue());
        Assert.assertEquals("改名后的标题", loaded.get(0).title);
        Assert.assertEquals(SECTION_ID, loaded.get(0).sectionId);
    }

    @Test
    public void testDeleteMissingEntityReturnsFalse() {
        IPluginContext pluginContext = Mockito.mock(IPluginContext.class);

        WorkshopWidgetStore widgetStore = WorkshopWidgetStore.create(ONTOLOGY_DOMAIN_ID, MODULE_NAME);
        Assert.assertFalse(widgetStore.delete(pluginContext, Optional.empty(), "不存在的 widget"));

        WorkshopSectionStore sectionStore = WorkshopSectionStore.create(ONTOLOGY_DOMAIN_ID, MODULE_NAME);
        Assert.assertFalse(sectionStore.delete(pluginContext, Optional.empty(),
                "44444444-4444-4444-4444-444444444444"));
    }

    private static WorkshopSection section(String id, String name, int sortOrder) {
        WorkshopSection section = new WorkshopSection();
        section.id = id;
        section.name = name;
        section.sortOrder = sortOrder;
        section.parent = WorkshopCptParentId.pageParent(PAGE_ID);
        return section;
    }

    private static TextInputWidget widget(String id, String sectionId) {
        TextInputWidget widget = new TextInputWidget();
        widget.id = id;
        widget.title = id;
        widget.sectionId = sectionId;
        return widget;
    }
}
