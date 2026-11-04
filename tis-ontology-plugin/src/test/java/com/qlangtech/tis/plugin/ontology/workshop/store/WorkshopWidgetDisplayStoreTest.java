package com.qlangtech.tis.plugin.ontology.workshop.store;

import com.qlangtech.tis.extension.impl.XmlFile;
import com.qlangtech.tis.plugin.ontology.workshop.WorkshopModule;
import com.qlangtech.tis.plugin.ontology.workshop.widget.AbstractWidgetDisplayConfig;
import com.qlangtech.tis.plugin.ontology.workshop.widget.AbsoluteSizingMode;
import com.qlangtech.tis.plugin.ontology.workshop.widget.AutoSizingMode;
import com.qlangtech.tis.plugin.ontology.workshop.widget.CompactDisplayConfig;
import com.qlangtech.tis.plugin.ontology.workshop.widget.WidgetDisplayConfig;
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
 * {@link WorkshopWidgetDisplayStore} 落盘链路的回归测试。
 *
 * <p>显示配置是 widget 的<b>独立插件实体</b>，落在 <b>与 widget 同一个</b> {@code widgets/}
 * 目录里（文件名 {@code {widgetId}_display.xml}）。共用目录带来三件事必须锁住：
 * <ol>
 *   <li>文件名确实是 {@code {widgetId}_display.xml}，顶层类名是两个 impl 之一
 *       （不是 widget 自己）；</li>
 *   <li>{@link WorkshopWidgetStore#listAll()} 不能把 display 文件解析成 widget；</li>
 *   <li>{@link AbstractWorkshopEntityStore#deleteNotIn(Set)} 两个方向都不能误删对方的文件 ——
 *       widget 的差集删除是 {@code doSaveModule} 最常走的一条路径，写漏了就是「保存一次
 *       显示配置全丢」。</li>
 * </ol>
 * 另外覆盖两个 impl 的往返读写（{@code _display.xml} 换 impl 后由 {@code PluginStore} 按
 * 根元素重新实例化）。
 */
public class WorkshopWidgetDisplayStoreTest {

    private static final String ONTOLOGY_DOMAIN_ID = "test_workshop_display_domain";
    private static final String MODULE_NAME = "test_workshop_display_module";

    private static final String SECTION_ID = "55555555-5555-5555-5555-555555555555";
    private static final String WIDGET_ID = "66666666-6666-6666-6666-666666666666";
    private static final String WIDGET_ID_2 = "77777777-7777-7777-7777-777777777777";

    @After
    public void cleanUp() {
        FileUtils.deleteQuietly(WorkshopModule.getWorkshopDir(ONTOLOGY_DOMAIN_ID, MODULE_NAME));
    }

    /**
     * 落盘文件名 = widget id + {@code _display} 后缀，与 widget 自己的 {@code {id}.xml} 同目录共存。
     */
    @Test
    public void testDisplayFileIsSuffixedNextToWidget() throws Exception {
        IPluginContext pluginContext = Mockito.mock(IPluginContext.class);
        WorkshopWidgetStore widgetStore = WorkshopWidgetStore.create(ONTOLOGY_DOMAIN_ID, MODULE_NAME);
        WorkshopWidgetDisplayStore displayStore = WorkshopWidgetDisplayStore.create(ONTOLOGY_DOMAIN_ID, MODULE_NAME);

        widgetStore.save(pluginContext, Optional.empty(), widget(WIDGET_ID));
        displayStore.save(pluginContext, Optional.empty(), fullDisplay(WIDGET_ID));

        File widgetFile = widgetStore.getEntityFile(WIDGET_ID);
        File displayFile = displayStore.getEntityFile(WIDGET_ID);
        Assert.assertEquals("display 与 widget 必须同目录",
                widgetFile.getParentFile(), displayFile.getParentFile());
        Assert.assertEquals(WIDGET_ID + WorkshopWidgetDisplayStore.DISPLAY_SUFFIX
                + XmlFile.KEY_XML_DOT_EXTENSION, displayFile.getName());
        Assert.assertTrue(widgetFile.isFile());
        Assert.assertTrue(displayFile.isFile());

        // 元素名是 FQCN（XStream 未开注解别名）
        String xml = FileUtils.readFileToString(displayFile, StandardCharsets.UTF_8);
        Assert.assertTrue("顶层元素应是显示配置 impl 而不是 widget，实际内容：\n" + xml,
                xml.contains("<" + WidgetDisplayConfig.class.getName() + ">"));
        Assert.assertFalse("widget 文件内容不得混进 display 文件，实际内容：\n" + xml,
                xml.contains("<" + TextInputWidget.class.getName() + ">"));
    }

    /**
     * 共用目录下 {@code WorkshopWidgetStore.listAll()} 只能返回 widget；
     * display 文件若混进来会被解析成 widget 实例（字段全 null），轻则画布多出脏 widget。
     */
    @Test
    public void testWidgetListAllExcludesDisplayFiles() {
        IPluginContext pluginContext = Mockito.mock(IPluginContext.class);
        WorkshopWidgetStore widgetStore = WorkshopWidgetStore.create(ONTOLOGY_DOMAIN_ID, MODULE_NAME);
        WorkshopWidgetDisplayStore displayStore = WorkshopWidgetDisplayStore.create(ONTOLOGY_DOMAIN_ID, MODULE_NAME);

        widgetStore.save(pluginContext, Optional.empty(), widget(WIDGET_ID));
        widgetStore.save(pluginContext, Optional.empty(), widget(WIDGET_ID_2));
        displayStore.save(pluginContext, Optional.empty(), fullDisplay(WIDGET_ID));

        List<WorkshopWidget> widgets = widgetStore.listAll();
        Assert.assertEquals("display 文件不应出现在 widget 列表里", 2, widgets.size());
        for (WorkshopWidget loaded : widgets) {
            Assert.assertNotNull("widget 不应是 display 文件解析出来的空壳", loaded.title);
        }

        Assert.assertEquals("display 列表同样只认自己的文件", 1, displayStore.listAll().size());
        // 所属 widget 取自实例上的 widgetId（identity），与文件名反解的结果一致
        Assert.assertEquals(Set.of(WIDGET_ID), displayStore.listAllByWidgetId().keySet());
    }

    /**
     * widget 的差集删除（{@code doSaveModule} 里最常走的路径）不得碰到 display 文件。
     */
    @Test
    public void testWidgetDiffDeleteKeepsDisplayFiles() {
        IPluginContext pluginContext = Mockito.mock(IPluginContext.class);
        WorkshopWidgetStore widgetStore = WorkshopWidgetStore.create(ONTOLOGY_DOMAIN_ID, MODULE_NAME);
        WorkshopWidgetDisplayStore displayStore = WorkshopWidgetDisplayStore.create(ONTOLOGY_DOMAIN_ID, MODULE_NAME);

        widgetStore.save(pluginContext, Optional.empty(), widget(WIDGET_ID));
        widgetStore.save(pluginContext, Optional.empty(), widget(WIDGET_ID_2));
        displayStore.save(pluginContext, Optional.empty(), fullDisplay(WIDGET_ID));
        displayStore.save(pluginContext, Optional.empty(), fullDisplay(WIDGET_ID_2));

        // 快照里只剩 WIDGET_ID_2：WIDGET_ID 的 widget 文件应被删，它的 display 不属于本 store，不能动
        Set<String> removed = widgetStore.deleteNotIn(Set.of(WIDGET_ID_2));
        Assert.assertEquals(Set.of(WIDGET_ID), removed);
        Assert.assertFalse(widgetStore.getEntityFile(WIDGET_ID).isFile());
        Assert.assertTrue("widget 差集删除不得删掉 display 文件",
                displayStore.getEntityFile(WIDGET_ID).isFile());
        Assert.assertTrue(displayStore.getEntityFile(WIDGET_ID_2).isFile());
    }

    /**
     * 反向：display 的差集删除同样只能删 {@code _display.xml}。
     */
    @Test
    public void testDisplayDiffDeleteKeepsWidgetFiles() {
        IPluginContext pluginContext = Mockito.mock(IPluginContext.class);
        WorkshopWidgetStore widgetStore = WorkshopWidgetStore.create(ONTOLOGY_DOMAIN_ID, MODULE_NAME);
        WorkshopWidgetDisplayStore displayStore = WorkshopWidgetDisplayStore.create(ONTOLOGY_DOMAIN_ID, MODULE_NAME);

        widgetStore.save(pluginContext, Optional.empty(), widget(WIDGET_ID));
        widgetStore.save(pluginContext, Optional.empty(), widget(WIDGET_ID_2));
        displayStore.save(pluginContext, Optional.empty(), fullDisplay(WIDGET_ID));
        displayStore.save(pluginContext, Optional.empty(), fullDisplay(WIDGET_ID_2));

        Set<String> removed = displayStore.deleteNotIn(Set.of(WIDGET_ID_2));
        Assert.assertEquals(Set.of(WIDGET_ID), removed);
        Assert.assertFalse(displayStore.getEntityFile(WIDGET_ID).isFile());
        Assert.assertTrue("display 差集删除不得删掉 widget 文件",
                widgetStore.getEntityFile(WIDGET_ID).isFile());
        Assert.assertTrue(widgetStore.getEntityFile(WIDGET_ID_2).isFile());
    }

    /**
     * 两个 impl 的往返读写：换 impl 是覆盖同一个文件（文件名只由 widget id 决定），
     * 读回时按根元素还原成对应的具体类。
     */
    @Test
    public void testRoundTripBothImpls() {
        IPluginContext pluginContext = Mockito.mock(IPluginContext.class);
        WorkshopWidgetDisplayStore displayStore = WorkshopWidgetDisplayStore.create(ONTOLOGY_DOMAIN_ID, MODULE_NAME);

        displayStore.save(pluginContext, Optional.empty(), fullDisplay(WIDGET_ID));
        AbstractWidgetDisplayConfig loaded = displayStore.load(WIDGET_ID);
        Assert.assertTrue("应读回完整显示配置，实际：" + loaded, loaded instanceof WidgetDisplayConfig);
        WidgetDisplayConfig full = (WidgetDisplayConfig) loaded;
        Assert.assertEquals(WidgetDisplayConfig.DisplayOptimization.neverUnmount, full.displayOptimization);
        Assert.assertEquals(Integer.valueOf(400), ((AbsoluteSizingMode) full.width).pixels);
        Assert.assertEquals("身份字段要跟着往返", WIDGET_ID, full.identityValue());

        // 切 impl：同一个 widget 的显示配置是覆盖写，不是新增第二个文件
        CompactDisplayConfig compact = compact(WIDGET_ID);
        AbsoluteSizingMode width = new AbsoluteSizingMode();
        width.pixels = 200;
        compact.width = width;
        displayStore.save(pluginContext, Optional.empty(), compact);

        Assert.assertEquals("换 impl 是覆盖写，不应多出文件", 1, displayStore.listAll().size());
        AbstractWidgetDisplayConfig reloaded = displayStore.load(WIDGET_ID);
        Assert.assertTrue("应读回精简显示配置，实际：" + reloaded, reloaded instanceof CompactDisplayConfig);
        Assert.assertEquals(Integer.valueOf(200), ((AbsoluteSizingMode) ((CompactDisplayConfig) reloaded).width).pixels);
    }

    /**
     * 删除 widget 时先删它的 display（{@code doApplyModule} 的顺序），删除后读回 {@code null}；
     * 删除不存在的实体返回 {@code false}。
     */
    @Test
    public void testDeleteThenLoadReturnsNull() {
        IPluginContext pluginContext = Mockito.mock(IPluginContext.class);
        WorkshopWidgetDisplayStore displayStore = WorkshopWidgetDisplayStore.create(ONTOLOGY_DOMAIN_ID, MODULE_NAME);

        displayStore.save(pluginContext, Optional.empty(), fullDisplay(WIDGET_ID));
        Assert.assertNotNull(displayStore.load(WIDGET_ID));

        Assert.assertTrue(displayStore.delete(pluginContext, Optional.empty(), WIDGET_ID));
        Assert.assertNull("删除后不应还能读到 display 配置", displayStore.load(WIDGET_ID));
        Assert.assertFalse(displayStore.delete(pluginContext, Optional.empty(), WIDGET_ID));
    }

    /**
     * 存量 identity（旧版可编辑名称，含中文 / 空格）同样要能落盘 —— 编码规则与 widget 一致，
     * 否则老 widget 一旦配了 Display 就会因非法文件名抛异常。
     */
    @Test
    public void testLegacyWidgetIdIsEncodedAsStorageKey() {
        IPluginContext pluginContext = Mockito.mock(IPluginContext.class);
        WorkshopWidgetDisplayStore displayStore = WorkshopWidgetDisplayStore.create(ONTOLOGY_DOMAIN_ID, MODULE_NAME);

        String legacyId = "对象表格 (副本)";
        displayStore.save(pluginContext, Optional.empty(), fullDisplay(legacyId));

        File expected = new File(displayStore.getStoreDir(),
                AbstractWorkshopEntityStore.encodeIdentity(legacyId) + WorkshopWidgetDisplayStore.DISPLAY_SUFFIX
                        + XmlFile.KEY_XML_DOT_EXTENSION);
        Assert.assertTrue("应以编码后的文件名落盘: " + expected.getAbsolutePath(), expected.isFile());

        AbstractWidgetDisplayConfig loaded = displayStore.load(legacyId);
        Assert.assertNotNull(loaded);
        // 实例上的身份是原值（编码只体现在文件名上）
        Assert.assertEquals(legacyId, loaded.identityValue());
        // 反解（URLDecoder）同样要能还原中文 / 空格 —— 加载路径靠它把文件挂回 widget
        Assert.assertTrue("listAllByWidgetId 的键应还原成原始 widget id",
                displayStore.listAllByWidgetId().containsKey(legacyId));
    }

    /**
     * 2026-10-31 那一版没有写 {@code <widgetId>}，存量文件读回后字段是 {@code null}；
     * 加载时按文件名回填，否则 {@code identityValue()} 抛异常会让整个模块加载失败。
     */
    @Test
    public void testLegacyFileWithoutWidgetIdIsBackFilledFromFileName() throws Exception {
        WorkshopWidgetDisplayStore displayStore = WorkshopWidgetDisplayStore.create(ONTOLOGY_DOMAIN_ID, MODULE_NAME);
        File storeDir = displayStore.getStoreDir();
        Assert.assertTrue(storeDir.isDirectory() || storeDir.mkdirs());

        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                + "<" + WidgetDisplayConfig.class.getName() + ">\n"
                + "  <width class=\"com.qlangtech.tis.plugin.ontology.workshop.widget.AbsoluteSizingMode\">\n"
                + "    <pixels>400</pixels>\n"
                + "  </width>\n"
                + "</" + WidgetDisplayConfig.class.getName() + ">\n";
        File legacyFile = new File(storeDir,
                WIDGET_ID + WorkshopWidgetDisplayStore.DISPLAY_SUFFIX + XmlFile.KEY_XML_DOT_EXTENSION);
        FileUtils.writeStringToFile(legacyFile, xml, StandardCharsets.UTF_8);

        AbstractWidgetDisplayConfig loaded = displayStore.load(WIDGET_ID);
        Assert.assertNotNull("存量文件应能读回", loaded);
        Assert.assertEquals("widgetId 应从文件名回填", WIDGET_ID, loaded.identityValue());
        Assert.assertEquals(Integer.valueOf(400),
                ((AbsoluteSizingMode) ((WidgetDisplayConfig) loaded).width).pixels);
        Assert.assertEquals(Set.of(WIDGET_ID), displayStore.listAllByWidgetId().keySet());
    }

    private static TextInputWidget widget(String id) {
        TextInputWidget widget = new TextInputWidget();
        widget.id = id;
        widget.title = id;
        widget.sectionId = SECTION_ID;
        return widget;
    }

    /** 一份完整显示配置 —— {@code widgetId} 是实体自己的 identity，落盘文件名由它决定 */
    private static WidgetDisplayConfig fullDisplay(String widgetId) {
        WidgetDisplayConfig display = new WidgetDisplayConfig();
        display.widgetId = widgetId;
        AbsoluteSizingMode width = new AbsoluteSizingMode();
        width.pixels = 400;
        display.width = width;
        display.height = new AutoSizingMode();
        display.displayOptimization = WidgetDisplayConfig.DisplayOptimization.neverUnmount;
        return display;
    }

    private static CompactDisplayConfig compact(String widgetId) {
        CompactDisplayConfig display = new CompactDisplayConfig();
        display.widgetId = widgetId;
        return display;
    }
}
