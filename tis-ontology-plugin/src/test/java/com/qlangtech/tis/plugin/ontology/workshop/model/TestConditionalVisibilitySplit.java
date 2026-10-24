package com.qlangtech.tis.plugin.ontology.workshop.model;

import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.workshop.TisPluginFormTestSupport;
import com.qlangtech.tis.plugin.ontology.workshop.widget.CompactDisplayConfig;
import com.qlangtech.tis.plugin.ontology.workshop.widget.WidgetButtonConfig;
import com.qlangtech.tis.plugin.ontology.workshop.widget.WidgetDisplayConfig;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * {@code ConditionalVisibility} 多态化结构回归：抽象父类 + off / 默认两个子类。
 *
 * <p>字段类型从具体类变为抽象父类后，下面任何一处漂移都是<b>静默失效</b>，因此逐条钉死：
 * <ul>
 *   <li>少一个子类 → 表单下拉少一项，用户无法选择/关闭；</li>
 *   <li>把父类字段下移到子类或反之 → ordinal 撞号；</li>
 *   <li>{@code Validator.require} 丢失 → 「选了启用却不选变量」的空壳配置能保存；</li>
 *   <li>descriptor 提到父类上 → 两个子类 {@code getId()} 撞号，表单上下发同一 impl。</li>
 * </ul>
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/22
 */
public class TestConditionalVisibilitySplit {

    private static final String PKG = "com.qlangtech.tis.plugin.ontology.workshop.model";

    private static final java.util.Comparator<Class<?>> BY_CLASS_NAME =
            java.util.Comparator.comparing((Class<?> c) -> c.getName());

    private static final Set<Class<?>> EXPECTED_SUBCLASSES = expectedSubclasses();

    private static Set<Class<?>> expectedSubclasses() {
        Set<Class<?>> result = new TreeSet<>(BY_CLASS_NAME);
        result.add(ConditionalVisibility.class);
        result.add(ConditionalVisibilityOff.class);
        return result;
    }

    private static final Set<String> HOST_FIELDS = new TreeSet<>(Arrays.asList(
            "AbstractWorkshopSection.conditionalVisibility",
            "WidgetDisplayConfig.conditionalVisibility",
            "CompactDisplayConfig.conditionalVisibility",
            "WidgetButtonConfig.conditionalVisibility"));

    @Test
    public void concreteSubclassesAreExactlyOffAndDefault() {
        Set<Class<?>> actual = new TreeSet<>(BY_CLASS_NAME);
        actual.addAll(TisPluginFormTestSupport.concreteSubclassesOf(
                ConditionalVisibilityConfig.class, PKG));
        Assert.assertEquals("ConditionalVisibilityConfig 的具体子类应恰好是 off / 默认实现"
                        + "（少一个子类意味着表单下拉少一项，用户无法选择或关闭）",
                EXPECTED_SUBCLASSES, actual);
    }

    @Test
    public void parentAndOffSubclassDeclareNoFormFields() {
        Assert.assertTrue("ConditionalVisibilityConfig 应是抽象类",
                java.lang.reflect.Modifier.isAbstract(ConditionalVisibilityConfig.class.getModifiers()));
        Assert.assertEquals("抽象父类不得声明 @FormField —— 字段全在子类，"
                        + "否则与子类 ordinal 撞号",
                0, formFieldsOf(ConditionalVisibilityConfig.class).size());

        Assert.assertEquals("off 子类应是零字段（「不启用」不需要任何参数）",
                0, formFieldsOf(ConditionalVisibilityOff.class).size());
        Assert.assertNull("零字段的 off 子类不应有 .json（有 json 意味着有人给它加了字段）",
                TisPluginFormTestSupport.formResource(ConditionalVisibilityOff.class.getName()));
    }

    @Test
    public void defaultSubclassFieldsAreRequiredAndDescribed() {
        Map<String, FormField> fields = formFieldsOf(ConditionalVisibility.class);
        Assert.assertEquals("默认实现应恰有 variableId / stateIfFalse 两个表单字段",
                new TreeSet<>(Arrays.asList("variableId", "stateIfFalse")),
                new TreeSet<>(fields.keySet()));

        for (String name : new String[]{"variableId", "stateIfFalse"}) {
            Assert.assertTrue(ConditionalVisibility.class.getSimpleName() + "." + name
                            + " 必须带 Validator.require：多态化后「选默认实现」就是显式启用动作，"
                            + "此时字段不能留空（否则可保存 enabled + 空变量的空壳配置）",
                    Arrays.asList(fields.get(name).validate()).contains(Validator.require));
        }

        JSONObject resource = TisPluginFormTestSupport.formResource(ConditionalVisibility.class.getName());
        Assert.assertNotNull("默认实现声明了 @FormField，必须有 ConditionalVisibility.json"
                + "（缺失时表单 label/help 全空且无报错）", resource);
        for (String name : fields.keySet()) {
            Assert.assertNotNull("ConditionalVisibility.json 缺少字段 " + name + " 的条目",
                    resource.getJSONObject(name));
        }
    }

    @Test
    public void descriptorsHaveDistinctIdsAndExpectedDisplayNames() {
        Descriptor<?> off = TisPluginFormTestSupport.descriptorOf(ConditionalVisibilityOff.class);
        Assert.assertEquals("off 子类的 displayName 必须是 Descriptor.SWITCH_OFF（\"off\"）——"
                        + "宿主 .json 的 dftVal 按裸字符串匹配它",
                Descriptor.SWITCH_OFF, off.getDisplayName());
        Assert.assertEquals("off 子类的 descriptor id 应是自身类 FQCN",
                ConditionalVisibilityOff.class.getName(), off.getId());
        // 前端 impl 下拉的候选来源是 DescriptorExtensionList<ConditionalVisibilityConfig>，
        // 由 getT()（沿继承链取 Descriptor 的泛型实参）决定；泛型写错时不会编译报错，
        // 只是这个实现落进别的列表 —— 表现为下拉少一项。
        Assert.assertEquals("off descriptor 的泛型实参应是抽象父类",
                ConditionalVisibilityConfig.class, off.getT());

        Descriptor<?> enabled = TisPluginFormTestSupport.descriptorOf(ConditionalVisibility.class);
        Assert.assertEquals("默认实现的 displayName 必须保持 \"Conditional Visibility\""
                        + "（下拉项标题；dftVal 若仍指向它则依赖该字面量）",
                "Conditional Visibility", enabled.getDisplayName());
        Assert.assertEquals("默认实现的 descriptor id 应是自身类 FQCN",
                ConditionalVisibility.class.getName(), enabled.getId());
        Assert.assertEquals("默认实现 descriptor 的泛型实参也应是抽象父类",
                ConditionalVisibilityConfig.class, enabled.getT());

        Assert.assertNotEquals("两个子类的 descriptor id 不得相同（descriptor 被提到父类会撞号）",
                off.getId(), enabled.getId());
    }

    /**
     * 四个宿主的 {@code conditionalVisibility} 默认值必须是 off。
     *
     * <p>默认值若仍是「启用」，它与 {@code variableId} 的 {@code Validator.require} 组合会造出
     * <b>一个过不了校验的缺省值</b>：用户没碰这个字段也会被拦在保存之外。
     */
    @Test
    public void hostDefaultsAreOff() {
        for (Class<?> host : Arrays.asList(AbstractWorkshopSection.class,
                WidgetDisplayConfig.class, CompactDisplayConfig.class, WidgetButtonConfig.class)) {
            JSONObject resource = TisPluginFormTestSupport.formResource(host.getName());
            Assert.assertNotNull(host.getSimpleName() + ".json 应存在", resource);
            JSONObject visibility = resource.getJSONObject("conditionalVisibility");
            Assert.assertNotNull(host.getSimpleName() + ".json 应有 conditionalVisibility 条目", visibility);
            Assert.assertEquals(host.getSimpleName() + ".conditionalVisibility 的默认值应为 off"
                            + "（与 variableId 的 Validator.require 配套）",
                    Descriptor.SWITCH_OFF, visibility.getString("dftVal"));
        }
    }

    @Test
    public void hostFieldsDeclaredAsAbstractParent() {
        Set<String> actual = new TreeSet<>();
        for (Class<?> host : Arrays.asList(AbstractWorkshopSection.class,
                WidgetDisplayConfig.class, CompactDisplayConfig.class, WidgetButtonConfig.class)) {
            try {
                Field field = host.getField("conditionalVisibility");
                Assert.assertEquals(host.getSimpleName() + ".conditionalVisibility 的声明类型应为抽象父类"
                                + "（改成具体子类后 off 选项在类型层面不可表示）",
                        ConditionalVisibilityConfig.class, field.getType());
                actual.add(host.getSimpleName() + ".conditionalVisibility");
            } catch (NoSuchFieldException e) {
                Assert.fail(host.getSimpleName() + " 缺少 conditionalVisibility 字段");
            }
        }
        Assert.assertEquals("宿主字段清单应与预期一致（漏改/新增宿主都会在这里暴露）",
                HOST_FIELDS, actual);
    }

    /** 本类自身或祖先链上声明的 {@code @FormField} 字段（按字段名索引） */
    private static Map<String, FormField> formFieldsOf(Class<?> clazz) {
        List<Field> fields = new java.util.ArrayList<>();
        for (Class<?> c = clazz; c != null && c != Object.class; c = c.getSuperclass()) {
            fields.addAll(Arrays.asList(c.getDeclaredFields()));
        }
        return fields.stream()
                .filter(f -> f.getAnnotation(FormField.class) != null)
                .collect(Collectors.toMap(Field::getName, f -> f.getAnnotation(FormField.class)));
    }
}
