package com.qlangtech.tis.plugin.ontology.workshop.widget.impl;

import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.ValueChangePipe;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.ontology.workshop.TisPluginFormTestSupport;
import com.qlangtech.tis.plugin.ontology.workshop.widget.DefaultListSorting;
import com.qlangtech.tis.plugin.ontology.workshop.widget.DefaultObjectSelection;
import com.qlangtech.tis.plugin.ontology.workshop.widget.ListSorting;
import com.qlangtech.tis.plugin.ontology.workshop.widget.NoListSorting;
import com.qlangtech.tis.plugin.ontology.workshop.widget.NoObjectSelection;
import com.qlangtech.tis.plugin.ontology.workshop.widget.ObjectSelection;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * {@code ObjectListWidget} 的「排序 / 多选」两组字段从「开关 + 可空载荷」改成 off/on 多态后的契约测试。
 *
 * <p>三块，与 {@link TestObjectTableWidgetPagenator} 同构：
 * <ol>
 *   <li><b>结构契约</b> —— 宿主字段确为抽象基类类型、5 个旧字段已删、每族恰好两个具体子类且
 *       其中一个是 off（顺带钉住抽象基类不混进插件面板）；</li>
 *   <li><b>跨语言约定</b> —— 宿主 json 的 {@code dftVal} 逐字等于关闭子类的 {@code displayName}，
 *       零字段的子类没有 json、有字段的子类键集与字段集一致。</li>
 *   <li><b>静默失效面</b> —— {@code SELECTABLE} 字段的选项注册（漏了整张表单打不开）、
 *       {@code objectSetVar} 级联管道到 {@code listSorting.sortField} 的点分目标
 *       （写错只在运行期失效，编译期无信号）。</li>
 * </ol>
 *
 * <p>为什么不靠 {@code TestWidgetVariableContract} 兜住 {@code selectedObjectsVar}：那个测试只扫
 * widget 的 {@code getDeclaredFields()} 且按 {@code *Var} 后缀识别，字段下沉进子类后就不在它的
 * 覆盖范围里了。{@link #selectedObjectsVarIsRegisteredOnTheOnSubclass()} 补上这一格。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/11/10
 */
public class TestObjectListWidgetSortingAndSelection {

    /** 两组多态族所在的包 —— sezpoz 索引按包前缀圈定扫描范围 */
    private static final String WIDGET_PKG = "com.qlangtech.tis.plugin.ontology.workshop.widget";

    /** 被替换掉的 5 个扁平字段 */
    private static final List<String> LEGACY_FIELDS = Arrays.asList(
            "defaultSortField", "defaultSortOrder", "enableUserSorting",
            "enableMultiSelect", "selectedObjectsVar");

    // ===================================================================
    // 1. 结构契约
    // ===================================================================

    @Test
    public void listSortingFieldIsPolymorphic() throws Exception {
        Assert.assertEquals("ObjectListWidget.listSorting 应声明为抽象基类类型（多态）",
                ListSorting.class, ObjectListWidget.class.getField("listSorting").getType());
        Assert.assertTrue("抽象基类不应被实例化",
                java.lang.reflect.Modifier.isAbstract(ListSorting.class.getModifiers()));
    }

    @Test
    public void objectSelectionFieldIsPolymorphic() throws Exception {
        Assert.assertEquals("ObjectListWidget.objectSelection 应声明为抽象基类类型（多态）",
                ObjectSelection.class, ObjectListWidget.class.getField("objectSelection").getType());
        Assert.assertTrue("抽象基类不应被实例化",
                java.lang.reflect.Modifier.isAbstract(ObjectSelection.class.getModifiers()));
    }

    /**
     * 旧字段必须删干净 —— 留着会出现两套排序 / 多选配置，且旧的那套不在相关字段白名单里，
     * 对服务端是静默 no-op，从界面上分辨不出该信哪个。
     */
    @Test
    public void legacySortingAndSelectionFieldsAreGone() {
        Set<String> actual = new TreeSet<>();
        for (Field field : ObjectListWidget.class.getFields()) {
            actual.add(field.getName());
        }
        List<String> survivors = new ArrayList<>();
        for (String legacy : LEGACY_FIELDS) {
            if (actual.contains(legacy)) {
                survivors.add(legacy);
            }
        }
        Assert.assertTrue("ObjectListWidget 不应再有这些扁平字段：" + survivors, survivors.isEmpty());
    }

    @Test
    public void exactlyTwoListSortingSubclassesOneOfWhichIsOff() {
        assertExactlyTwoSubclassesOneOfWhichIsOff(ListSorting.class,
                new TreeSet<>(List.of("DefaultListSorting", "NoListSorting")));
    }

    @Test
    public void exactlyTwoObjectSelectionSubclassesOneOfWhichIsOff() {
        assertExactlyTwoSubclassesOneOfWhichIsOff(ObjectSelection.class,
                new TreeSet<>(List.of("DefaultObjectSelection", "NoObjectSelection")));
    }

    /**
     * 恰好两个具体子类，且其中一个的 {@code displayName} 就是 {@link Descriptor#SWITCH_OFF}。
     *
     * <p>顺带钉住「抽象基类不混进面板」：基类自己不带 {@code @TISExtension}，故不该作为子类出现在
     * 索引里（sezpoz 不过滤抽象类，一旦有人给基类也加上注解，面板上就会多出一个无法实例化的条目）。
     *
     * <p>不用 {@code TisPluginFormTestSupport.offSubclassOf} —— 它按 {@code None} 前缀找人，
     * 本工程的多态族用的是 {@code No…}（{@code NoPagenator} / {@code NoListSorting}）。
     */
    private static void assertExactlyTwoSubclassesOneOfWhichIsOff(Class<?> base, Set<String> expected) {
        Set<Class<?>> subs = TisPluginFormTestSupport.concreteSubclassesOf(base, WIDGET_PKG);
        Set<String> names = TisPluginFormTestSupport.names(subs);
        Assert.assertEquals(base.getSimpleName() + " 应恰好有两个具体子类（一个开启、一个关闭），实际：" + names,
                expected, names);

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
    // 2. 跨语言约定（表单资源）
    // ===================================================================

    /**
     * 宿主 json 的两个 {@code dftVal} 必须逐字等于对应关闭子类的 {@code displayName}（{@code "off"}）。
     *
     * <p>后端按这个字符串给新 Widget 选默认子类，前端 {@code resolveObjectListSorting} 也对它 ——
     * 写成 {@code "OFF"} / {@code "关闭"} 都不报错，只会静默落到开启态上，于是「新建的 object-list
     * 默认不排序、不多选」这个约定无声地失效了。
     */
    @Test
    public void hostFormResourceDefaultsToTheOffSubclass() {
        JSONObject resource = TisPluginFormTestSupport.formResource(ObjectListWidget.class.getName());
        Assert.assertNotNull("ObjectListWidget.json 应存在", resource);

        JSONObject sortingMeta = resource.getJSONObject(ObjectListWidget.KEY_LIST_SORTING);
        Assert.assertNotNull(ObjectListWidget.KEY_LIST_SORTING + " 在 ObjectListWidget.json 里应有条目",
                sortingMeta);
        Assert.assertEquals("dftVal 必须逐字等于关闭子类的 displayName",
                TisPluginFormTestSupport.descriptorOf(NoListSorting.class).getDisplayName(),
                sortingMeta.getString("dftVal"));
        Assert.assertEquals("关闭子类的 displayName 应当就是框架常量 SWITCH_OFF",
                Descriptor.SWITCH_OFF, sortingMeta.getString("dftVal"));

        JSONObject selectionMeta = resource.getJSONObject(ObjectListWidget.KEY_OBJECT_SELECTION);
        Assert.assertNotNull(ObjectListWidget.KEY_OBJECT_SELECTION + " 在 ObjectListWidget.json 里应有条目",
                selectionMeta);
        Assert.assertEquals("dftVal 必须逐字等于关闭子类的 displayName",
                TisPluginFormTestSupport.descriptorOf(NoObjectSelection.class).getDisplayName(),
                selectionMeta.getString("dftVal"));
        Assert.assertEquals("关闭子类的 displayName 应当就是框架常量 SWITCH_OFF",
                Descriptor.SWITCH_OFF, selectionMeta.getString("dftVal"));

        for (String legacy : LEGACY_FIELDS) {
            Assert.assertNull("旧的 " + legacy + " 条目应已从 json 里删除", resource.getJSONObject(legacy));
        }
    }

    /**
     * 零字段的关态子类<b>不该</b>有 json（写了是死文件：TIS 只在类上存在 {@code @FormField}
     * 时才去查同名资源）；有字段的开态子类<b>必须</b>有，且键集与字段集一致 —— 漏一个字段表现是
     * 「那个输入框没有 help / label」，不报错。
     */
    @Test
    public void formResourcesMatchFormFields() {
        assertFormResourceMatchesFields(NoListSorting.class, false);
        assertFormResourceMatchesFields(DefaultListSorting.class, true);
        assertFormResourceMatchesFields(NoObjectSelection.class, false);
        assertFormResourceMatchesFields(DefaultObjectSelection.class, true);
    }

    private static void assertFormResourceMatchesFields(Class<?> pluginClass, boolean hasFields) {
        JSONObject resource = TisPluginFormTestSupport.formResource(pluginClass.getName());
        if (!hasFields) {
            Assert.assertNull(pluginClass.getSimpleName() + " 零字段，不应有表单资源", resource);
            return;
        }
        Assert.assertNotNull(pluginClass.getSimpleName() + " 有字段，应有表单资源", resource);
        Assert.assertEquals("表单资源的键集应与 @FormField 字段集一致",
                formFieldNames(pluginClass), new TreeSet<>(resource.keySet()));
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

    // ===================================================================
    // 3. 静默失效面
    // ===================================================================

    /**
     * {@code sortField} 是 {@code SELECTABLE}，必须在开态子类的 descriptor 上注册候选来源 ——
     * 未注册时 {@code Descriptor.getSelectOptions} 抛 {@code has not been register}，整张表单打不开。
     *
     * <p>判定口径与 {@code TestWidgetVariableContract#everySelectableBindingFieldIsRegistered} 一致：
     * 只有消息含 {@code has not been register} 的 {@code IllegalStateException} 才算失败，其他异常
     * 说明 getter 已执行到取运行期上下文那一步（注册是存在的）。
     */
    @Test
    public void sortFieldIsRegisteredOnTheOnSubclass() {
        assertSelectOptionsRegistered(DefaultListSorting.class, DefaultListSorting.KEY_SORT_FIELD);
    }

    /** 同上。这条补上 {@code TestWidgetVariableContract} 扫不到下沉字段的缺口 */
    @Test
    public void selectedObjectsVarIsRegisteredOnTheOnSubclass() {
        assertSelectOptionsRegistered(DefaultObjectSelection.class,
                DefaultObjectSelection.KEY_SELECTED_OBJECTS_VAR);
    }

    private static void assertSelectOptionsRegistered(Class<?> pluginClass, String fieldName) {
        Descriptor<?> descriptor = TisPluginFormTestSupport.descriptorOf(pluginClass);
        Assert.assertNotNull(pluginClass.getSimpleName() + " 应有可实例化的 Descriptor 内部类", descriptor);
        try {
            descriptor.getSelectOptions(fieldName);
        } catch (IllegalStateException e) {
            Assert.assertFalse(pluginClass.getSimpleName() + "." + fieldName + " 未注册选项来源："
                            + e.getMessage(),
                    String.valueOf(e.getMessage()).contains("has not been register"));
        } catch (Throwable t) {
            // 已注册，只是选项 getter 求值需要 TIS 运行期上下文
        }
    }

    /**
     * {@code objectSetVar} 的级联管道必须包含点分的 {@code listSorting.sortField}。
     *
     * <p>目标字段下沉进子表单后，前端只认「宿主 vals 上的 {@code $pipe_field$listSorting.sortField}」。
     * 写成 {@code "sortField"}（漏前缀）或在 {@code render(...)} 里漏登记，都会在运行期静默失效 ——
     * 表现是「改了对象集变量，排序字段下拉还是空的」，本地跑不出任何错。
     */
    @Test
    public void objectSetVarPipeFeedsSortField() {
        ValueChangePipe pipe = new ObjectListWidget.DescriptorImpl()
                .getValueChangePipe(ObjectListWidget.KEY_OBJECT_SET_VAR);
        Set<String> toFields = new TreeSet<>(Arrays.asList(pipe.getToField()));
        String dotted = ObjectListWidget.KEY_LIST_SORTING + "." + DefaultListSorting.KEY_SORT_FIELD;
        Assert.assertTrue("级联目标应含 " + dotted + "，实际：" + toFields, toFields.contains(dotted));
        Assert.assertFalse("不应再有扁平的 sortField 目标（它已下沉进子表单）",
                toFields.contains(DefaultListSorting.KEY_SORT_FIELD));
    }

    // ===================================================================
    // 4. 两个子类的行为
    // ===================================================================

    @Test
    public void noListSortingDisablesEverything() {
        NoListSorting off = new NoListSorting();
        Assert.assertFalse("关态不应排序", off.sortingEnabled());
        Assert.assertNull("关态没有排序字段", off.resolveSortField());
        Assert.assertNull("关态没有排序方向", off.resolveSortOrder());
    }

    @Test
    public void noObjectSelectionDisablesMultiSelect() {
        NoObjectSelection off = new NoObjectSelection();
        Assert.assertFalse("关态不应支持多选", off.multiSelectEnabled());
        Assert.assertNull("关态没有写回变量", off.resolveSelectedObjectsVar());
    }

    /** {@code sortOrder} 的字段初值必须是 asc，且与 json 的 {@code dftVal} 一致 */
    @Test
    public void defaultListSortingDefaultsToAscending() {
        DefaultListSorting sorting = new DefaultListSorting();
        Assert.assertTrue(sorting.sortingEnabled());
        Assert.assertEquals(ListSorting.SortOrder.asc, sorting.sortOrder);
        Assert.assertEquals("未填时回落 asc", ListSorting.SortOrder.asc, sorting.resolveSortOrder());

        // 表单清空会落 null，也应当回落而不是 NPE（与 DefaultPagenator.resolvePageSize 同口径）
        sorting.sortOrder = null;
        Assert.assertEquals(ListSorting.SortOrder.asc, sorting.resolveSortOrder());

        JSONObject resource = TisPluginFormTestSupport.formResource(DefaultListSorting.class.getName());
        Assert.assertNotNull(resource);
        Assert.assertEquals("json 的 dftVal 应与 Java 初值逐字一致",
                ListSorting.SortOrder.asc.name(),
                resource.getJSONObject(DefaultListSorting.KEY_SORT_ORDER).getString("dftVal"));
    }

    @Test
    public void defaultObjectSelectionResolvesVar() {
        DefaultObjectSelection selection = new DefaultObjectSelection();
        Assert.assertTrue(selection.multiSelectEnabled());
        Assert.assertNull(selection.resolveSelectedObjectsVar());

        selection.selectedObjectsVar = "selectedObjects";
        Assert.assertEquals("selectedObjects", selection.resolveSelectedObjectsVar());
    }
}
