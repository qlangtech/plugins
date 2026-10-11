package com.qlangtech.tis.plugin.ontology.workshop.model;

import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.plugin.IPluginStore;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.ontology.workshop.TisPluginFormTestSupport;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * {@code WorkshopSection.childSections} 从自引用 {@code List<WorkshopSection>} 收敛为有界一层
 * {@code List<SectionChild>} 的回归测试。
 *
 * <p>这轮改造把一个「任意深度自引用」的字段换成了「有界一层」：{@link WorkshopSection} 与
 * {@link SectionChild} 共享 {@link AbstractWorkshopSection} 的全部字段，但 {@link SectionChild}
 * <b>不再有 {@code childSections}</b>，从而断掉 {@code WorkshopSection → childSections → …} 的自引用，
 * 规避 {@code DescriptorsJSON.getDescriptorsJSON()} 无环检测/无深度上限导致的 StackOverflowError。
 *
 * <p>全部断言<b>不依赖 TIS 运行期</b>（不需要 {@code --add-opens}）：descriptor 靠
 * {@link TisPluginFormTestSupport#descriptorOf} 反射 {@code newInstance}，字段图靠反射走裸类层次，
 * 与 {@code section.TestWorkshopSectionConfig} 的取舍一致，避免整类因环境问题变成「一直绿但什么都没查」。
 */
public class TestWorkshopSectionChildSections {

    /** 六个共享字段 —— 与前端 {@code SectionBase} 接口及 {@code AbstractWorkshopSection.json} 逐字对应 */
    private static final Set<String> SHARED_FIELDS = new TreeSet<>(Arrays.asList(
            "name", "layout", "styleConfig", "conditionalVisibility", "dropHandling", "sortOrder"));

    // ===================================================================
    // 1. 断环：childSections 元素必须是 SectionChild，且 SectionChild 不得再引用 section
    // ===================================================================

    /**
     * {@code childSections} 的元素类型必须是 {@link SectionChild}，而不是 {@link WorkshopSection} 自身。
     *
     * <p>这是断环的第一步：元素类型回到 {@code WorkshopSection} 就等于又拼回了自引用。
     */
    @Test
    public void childSectionsElementIsSectionChildNotSelf() {
        Field f = field(WorkshopSection.class, "childSections");
        Assert.assertEquals("childSections 应是 List 字段", List.class, f.getType());

        Type generic = f.getGenericType();
        Assert.assertTrue("childSections 应是带泛型参数的 List", generic instanceof ParameterizedType);
        Type element = ((ParameterizedType) generic).getActualTypeArguments()[0];
        Assert.assertEquals("childSections 的元素类型必须是 SectionChild（回到 WorkshopSection 即自引用复辟）",
                SectionChild.class, element);
    }

    /**
     * {@link SectionChild} 及其基类 {@link AbstractWorkshopSection} 上，<b>不得有任何字段</b>
     * （直接或 List 元素）引用回 section 类型（{@code AbstractWorkshopSection} 或其子类）。
     *
     * <p>这是断环的第二步、也是核心保证：只要 SectionChild 这一层不再引用 section，描述符 JSON
     * 下钻到 SectionChild 就自然终止，不会绕回 WorkshopSection。字段类型判据用
     * {@code AbstractWorkshopSection.class.isAssignableFrom(t)}，天然覆盖 WorkshopSection / SectionChild。
     */
    @Test
    public void sectionChildHasNoFieldReferencingSection() {
        List<String> problems = new ArrayList<>();
        for (Field f : allInstanceFields(SectionChild.class)) {
            Class<?> direct = f.getType();
            Class<?> element = listElementType(f);
            if (isSectionType(direct) || isSectionType(element)) {
                problems.add("字段 " + f.getName() + " 的类型 "
                        + (direct != null ? direct.getSimpleName() : "?") + " 引用回了 section 类型"
                        + "（childSections 的断环被重新拼上，描述符 JSON 会 StackOverflowError）");
            }
        }
        Assert.assertTrue(String.join("\n", problems), problems.isEmpty());
    }

    // ===================================================================
    // 2. MULTI_DESCRIBLE_PLUGIN 契约
    // ===================================================================

    /**
     * {@code childSections} 必须是 {@code MULTI_DESCRIBLE_PLUGIN}，且元素类 {@link SectionChild}
     * 实现 {@link IPluginStore.MultiDescribleElement}，descriptor 默认开放集合 + List 视图。
     *
     * <p>漏实现 {@code MultiDescribleElement} 时表单能渲染但保存/回填阶段才炸（见
     * {@code PropertyType.createFieldListElementClazz} 的强校验）。
     */
    @Test
    public void childSectionsFieldIsMultiDescriblePlugin() {
        FormField ff = field(WorkshopSection.class, "childSections").getAnnotation(FormField.class);
        Assert.assertNotNull("childSections 应有 @FormField 注解", ff);
        Assert.assertEquals("childSections 应是 MULTI_DESCRIBLE_PLUGIN 类型",
                FormFieldType.MULTI_DESCRIBLE_PLUGIN, ff.type());
        Assert.assertEquals("childSections 的 desClazz 应是 SectionChild",
                SectionChild.class, ff.desClazz());

        Assert.assertTrue("SectionChild 必须实现 MultiDescribleElement（= ManipuldateProcessor + IdentityName）",
                IPluginStore.MultiDescribleElement.class.isAssignableFrom(SectionChild.class));

        Descriptor<?> desc = TisPluginFormTestSupport.descriptorOf(SectionChild.class);
        Assert.assertTrue("SectionChild 的 descriptor 应是 MultiDescribleElementDescriptor",
                desc instanceof IPluginStore.MultiDescribleElementDescriptor);

        IPluginStore.MultiDescribleElementDescriptor<?> md =
                (IPluginStore.MultiDescribleElementDescriptor<?>) desc;
        Assert.assertFalse("子分区是开放集合（自由添加），isEnumableSet() 应为 false", md.isEnumableSet());
        Assert.assertEquals("子分区默认 List 视图",
                IPluginStore.MultiDescribleElementDescriptor.ViewStyle.List, md.viewStyle());
    }

    // ===================================================================
    // 3. 基类共享字段对两个子类都生效（祖先链收集对非 Describable 基类的关键验证）
    // ===================================================================

    /**
     * 六个共享字段对 {@link WorkshopSection} 与 {@link SectionChild} 两个 descriptor 都齐，
     * 且只有 {@link WorkshopSection} 多出 {@code childSections}。
     *
     * <p>这是「基类不实现 Describable、只承载 {@code @FormField} 字段」这条设计的落点验证：
     * {@code @FormField} 按祖先链反射收集，与基类是否 Describable 无关。
     */
    @Test
    public void sharedFieldsCollectedForBothSectionTypes() {
        Set<String> ws = TisPluginFormTestSupport.descriptorOf(WorkshopSection.class)
                .getPropertyTypes().keySet();
        Set<String> sc = TisPluginFormTestSupport.descriptorOf(SectionChild.class)
                .getPropertyTypes().keySet();

        Assert.assertTrue("WorkshopSection 的表单属性应包含六个共享字段，实际：" + ws,
                ws.containsAll(SHARED_FIELDS));
        Assert.assertTrue("SectionChild 的表单属性应包含六个共享字段，实际：" + sc,
                sc.containsAll(SHARED_FIELDS));

        Assert.assertTrue("WorkshopSection 应额外有 childSections 字段，实际：" + ws,
                ws.contains("childSections"));
        Assert.assertFalse("SectionChild 不应有 childSections（有界一层的关键），实际：" + sc,
                sc.contains("childSections"));
    }

    // ===================================================================
    //  helpers
    // ===================================================================

    private static boolean isSectionType(Class<?> t) {
        return t != null && AbstractWorkshopSection.class.isAssignableFrom(t);
    }

    private static Field field(Class<?> clazz, String name) {
        try {
            return clazz.getField(name);
        } catch (NoSuchFieldException e) {
            throw new AssertionError(clazz.getSimpleName() + " 缺少字段 " + name, e);
        }
    }

    /** 字段类型若是 List/数组，返回其元素/组件类型；否则返回 null */
    private static Class<?> listElementType(Field f) {
        Type generic = f.getGenericType();
        if (generic instanceof ParameterizedType) {
            Type[] args = ((ParameterizedType) generic).getActualTypeArguments();
            if (args.length == 1 && args[0] instanceof Class) {
                return (Class<?>) args[0];
            }
        }
        if (f.getType().isArray()) {
            return f.getType().getComponentType();
        }
        return null;
    }

    private static List<Field> allInstanceFields(Class<?> clazz) {
        List<Field> result = new ArrayList<>();
        for (Class<?> c = clazz; c != null && c != Object.class; c = c.getSuperclass()) {
            result.addAll(Arrays.asList(c.getDeclaredFields()));
        }
        return result;
    }
}
