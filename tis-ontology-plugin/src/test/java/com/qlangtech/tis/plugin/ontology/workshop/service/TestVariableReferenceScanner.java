package com.qlangtech.tis.plugin.ontology.workshop.service;

import com.qlangtech.tis.TIS;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.plugin.ontology.workshop.WorkshopModule;
import com.qlangtech.tis.plugin.ontology.workshop.model.ConditionalVisibility;
import com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopPage;
import com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopSection;
import com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopVariable;
import com.qlangtech.tis.plugin.ontology.workshop.model.definition.ObjectSetDefinitionConfig;
import com.qlangtech.tis.plugin.ontology.workshop.model.section.LoopLayout;
import com.qlangtech.tis.plugin.ontology.workshop.model.variable.ObjectSetVariable;
import com.qlangtech.tis.plugin.ontology.workshop.model.variable.StringVariable;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.ObjectTableWidget;
import org.junit.Assume;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * {@link VariableReferenceScanner} 的引用识别测试：纯对象图反射，不依赖 TIS 运行时。
 *
 * <p>扫的是「引用 = 字段名像绑定字段 + 值等于变量名」这条谓词的两面：
 * 该报的（widget 绑定 / 条件可见性 / 对象集过滤 / 循环迭代变量 / 页头组件 / 他变量配置）都报，
 * 不该报的（{@code itemVariable} 声明、被删变量自己的配置子树）不报。
 * 变量名比较是忽略大小写的（引用约定按名字绑定，{@link WorkshopVariable#normalizeName}）。
 */
public class TestVariableReferenceScanner {

    private static final String VAR_NAME = "Region_List";

    @Test
    public void widgetBindingAndNestedConfigsAreReported() {
        WorkshopModule module = new WorkshopModule();
        module.pages.add(pageWithWidget("销售看板", "KPI 分区", (widget) -> {
            widget.objectSetVar = VAR_NAME;
            widget.activeObjectVar = "其他变量";
            ConditionalVisibility visibility = new ConditionalVisibility();
            visibility.variableId = "region_list"; // 故意小写：引用比较忽略大小写
            widget.conditionalVisibility = visibility;
        }));

        List<String> hits = VariableReferenceScanner.scan(module, VAR_NAME);

        assertEquals(2, hits.size());
        assertTrue(hits.stream().anyMatch((h) -> h.contains("页面「销售看板」")
                && h.contains("分区「KPI 分区」")
                && h.contains("组件「明细表」")
                && h.contains(" : objectSetVar")));
        assertTrue(hits.stream().anyMatch((h) -> h.contains("conditionalVisibility")
                && h.contains(" : variableId")));
        assertFalse(hits.stream().anyMatch((h) -> h.contains("activeObjectVar")));
    }

    @Test
    public void loopIteratorIsReferenceButItemVariableIsDeclaration() {
        WorkshopModule module = new WorkshopModule();
        module.pages.add(pageWithWidget("首页", "循环区", (widget) -> { }));
        WorkshopSection section = module.pages.get(0).sections.get(0);
        LoopLayout loop = new LoopLayout();
        // itemVariable 与被删变量同名也不算引用：它是循环声明的合成变量，不是消费
        loop.iteratorVariable = VAR_NAME;
        loop.itemVariable = VAR_NAME;
        section.layout = loop;

        List<String> hits = VariableReferenceScanner.scan(module, VAR_NAME);

        assertEquals(1, hits.size());
        assertTrue(hits.get(0).contains(" > layout : iteratorVariable"));
        assertFalse(hits.stream().anyMatch((h) -> h.contains("itemVariable")));
    }

    @Test
    public void otherVariablesConfigSubtreeIsScanned() {
        WorkshopModule module = new WorkshopModule();
        ObjectSetVariable objectSetVar = new ObjectSetVariable();
        objectSetVar.name = "汇总对象集";
        ObjectSetDefinitionConfig definition = new ObjectSetDefinitionConfig();
        definition.filterVar = VAR_NAME;
        objectSetVar.definitionConfig = definition;
        module.variables.add(objectSetVar);
        // 被删变量自己也要在列表里（真实删除场景），且它自己的配置子树即使含同值字段也不报
        StringVariable victim = new StringVariable();
        victim.name = VAR_NAME;
        ObjectSetDefinitionConfig selfRef = new ObjectSetDefinitionConfig();
        selfRef.filterVar = VAR_NAME;
        victim.definitionConfig = selfRef;
        module.variables.add(victim);

        List<String> hits = VariableReferenceScanner.scan(module, VAR_NAME);

        assertEquals(1, hits.size());
        assertTrue(hits.get(0).contains("变量「汇总对象集」")
                && hits.get(0).contains("definitionConfig")
                && hits.get(0).contains(" : filterVar"));
    }

    @Test
    public void headerWidgetIsReported() {
        WorkshopModule module = new WorkshopModule();
        ObjectTableWidget headerWidget = new ObjectTableWidget();
        headerWidget.activeObjectVar = VAR_NAME;
        module.headerWidgets.add(headerWidget);

        List<String> hits = VariableReferenceScanner.scan(module, VAR_NAME);

        assertEquals(1, hits.size());
        assertTrue(hits.get(0).startsWith("页头 > "));
        assertTrue(hits.get(0).endsWith(" : activeObjectVar"));
    }

    @Test
    public void noReferenceYieldsEmptyList() {
        WorkshopModule module = new WorkshopModule();
        module.pages.add(pageWithWidget("首页", "主区", (widget) -> widget.objectSetVar = "别的变量"));

        assertTrue(VariableReferenceScanner.scan(module, VAR_NAME).isEmpty());
    }

    /**
     * 生产路径（表单属性元数据遍历）的显式验证：TIS 就绪时节点全部走
     * {@code getDescriptor().getPropertyTypes} 下钻，本测试钉住「绑定字段都在声明的
     * 表单属性里」——{@code conditionalVisibility} / {@code layout} 若哪天被改出
     * {@code @FormField}（元数据看不到、只剩 transient 补扫/兜底能盖住的情况不存在：
     * 它们非 transient），此测试即红。裸 surefire 环境（无 TIS）跳过，由上面几个
     * 纯反射兜底测试钉住同一组行为。
     */
    @Test
    public void metadataTraversalCoversFormBindingFieldsWhenTisReady() {
        Descriptor<?> widgetDesc;
        try {
            TIS tis = TIS.get();
            widgetDesc = tis == null ? null : tis.getDescriptor(ObjectTableWidget.class);
        } catch (Throwable t) {
            widgetDesc = null;
        }
        Assume.assumeTrue("当前环境未初始化 TIS（JDK17 下通常还缺 "
                + "--add-opens java.base/java.lang=ALL-UNNAMED），元数据路径无从验证，跳过",
                widgetDesc != null);

        WorkshopModule module = new WorkshopModule();
        module.pages.add(pageWithWidget("销售看板", "KPI 分区", (widget) -> {
            widget.objectSetVar = VAR_NAME;
            ConditionalVisibility visibility = new ConditionalVisibility();
            visibility.variableId = "region_list";
            widget.conditionalVisibility = visibility;
        }));
        WorkshopSection section = module.pages.get(0).sections.get(0);
        LoopLayout loop = new LoopLayout();
        loop.iteratorVariable = VAR_NAME;
        section.layout = loop;

        List<String> hits = VariableReferenceScanner.scan(module, VAR_NAME);

        assertEquals(3, hits.size());
        assertTrue(hits.stream().anyMatch((h) -> h.contains("组件「明细表」")
                && h.contains(" : objectSetVar")));
        assertTrue(hits.stream().anyMatch((h) -> h.contains("conditionalVisibility")
                && h.contains(" : variableId")));
        assertTrue(hits.stream().anyMatch((h) -> h.contains(" > layout : iteratorVariable")));
    }

    // ===================================================================
    //  测试脚手架
    // ===================================================================

    private interface WidgetCustomizer {
        void customize(ObjectTableWidget widget);
    }

    private WorkshopPage pageWithWidget(String pageName, String sectionName, WidgetCustomizer customizer) {
        WorkshopPage page = new WorkshopPage();
        page.displayName = pageName;
        WorkshopSection section = new WorkshopSection();
        section.name = sectionName;
        ObjectTableWidget widget = new ObjectTableWidget();
        widget.title = "明细表";
        customizer.customize(widget);
        section.widgets = new ArrayList<>();
        section.widgets.add(widget);
        page.sections = new ArrayList<>();
        page.sections.add(section);
        return page;
    }
}
