package com.qlangtech.tis.plugin.ontology.workshop.model;

import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.workshop.TisPluginFormTestSupport;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * {@code AbstractSecondaryMetricConfig} 多态化结构回归：抽象父类 + off / 启用两个子类。
 *
 * <p>原来「是否显示副指标」是 {@code MetricConfig.showSecondaryMetric} 布尔开关，与配置对象
 * 是两个独立真值来源；改为多态后任何一处漂移都会让开关能力静默失效，因此逐条钉死：
 * <ul>
 *   <li>少一个子类 → 表单下拉少一项，用户无法选择/关闭；</li>
 *   <li>父类声明 {@code @FormField} 或子类漏字段 → ordinal 撞号；</li>
 *   <li>启用态丢失 {@code Validator.require} → 「选了启用却不选变量」的空壳配置能保存；</li>
 *   <li>descriptor 提到父类上 → 两个子类 {@code getId()} 撞号。</li>
 * </ul>
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/24
 */
public class TestSecondaryMetricSplit {

    private static final String PKG = "com.qlangtech.tis.plugin.ontology.workshop.model";

    private static final Comparator<Class<?>> BY_CLASS_NAME =
            Comparator.comparing((Class<?> c) -> c.getName());

    private static final Set<Class<?>> EXPECTED_SUBCLASSES = expectedSubclasses();

    private static Set<Class<?>> expectedSubclasses() {
        Set<Class<?>> result = new TreeSet<>(BY_CLASS_NAME);
        result.add(SecondaryMetricConfig.class);
        result.add(SecondaryMetricOff.class);
        return result;
    }

    @Test
    public void concreteSubclassesAreExactlyOffAndEnabled() {
        Set<Class<?>> actual = new TreeSet<>(BY_CLASS_NAME);
        actual.addAll(TisPluginFormTestSupport.concreteSubclassesOf(
                AbstractSecondaryMetricConfig.class, PKG));
        Assert.assertEquals("AbstractSecondaryMetricConfig 的具体子类应恰好是 off / 启用实现"
                        + "（少一个子类意味着表单下拉少一项，用户无法选择或关闭）",
                EXPECTED_SUBCLASSES, actual);
    }

    @Test
    public void parentAndOffSubclassDeclareNoFormFields() {
        Assert.assertTrue("AbstractSecondaryMetricConfig 应是抽象类",
                java.lang.reflect.Modifier.isAbstract(AbstractSecondaryMetricConfig.class.getModifiers()));
        Assert.assertEquals("抽象父类不得声明 @FormField —— 字段全在子类，"
                        + "否则与子类 ordinal 撞号",
                0, formFieldsOf(AbstractSecondaryMetricConfig.class).size());

        Assert.assertEquals("off 子类应是零字段（「不显示」不需要任何参数）",
                0, formFieldsOf(SecondaryMetricOff.class).size());
        Assert.assertNull("零字段的 off 子类不应有 .json（有 json 意味着有人给它加了字段）",
                TisPluginFormTestSupport.formResource(SecondaryMetricOff.class.getName()));
    }

    @Test
    public void enabledSubclassFieldsAreRequiredAndDescribed() {
        Map<String, FormField> fields = formFieldsOf(SecondaryMetricConfig.class);
        Assert.assertEquals("启用实现应恰有 label / valueType / valueVariable / numericFormatting 四个表单字段",
                new TreeSet<>(Arrays.asList("label", "valueType", "valueVariable", "numericFormatting")),
                new TreeSet<>(fields.keySet()));

        for (String name : new String[]{"valueType", "valueVariable"}) {
            Assert.assertTrue(SecondaryMetricConfig.class.getSimpleName() + "." + name
                            + " 必须带 Validator.require：多态化后「选启用」就是显式启用动作，"
                            + "此时字段不能留空（否则可保存 enabled + 空变量的空壳配置）",
                    Arrays.asList(fields.get(name).validate()).contains(Validator.require));
        }

        JSONObject resource = TisPluginFormTestSupport.formResource(SecondaryMetricConfig.class.getName());
        Assert.assertNotNull("启用实现声明了 @FormField，必须有 SecondaryMetricConfig.json"
                + "（缺失时表单 label/help 全空且无报错）", resource);
        for (String name : fields.keySet()) {
            Assert.assertNotNull("SecondaryMetricConfig.json 缺少字段 " + name + " 的条目",
                    resource.getJSONObject(name));
        }
    }

    @Test
    public void descriptorsHaveDistinctIdsAndExpectedDisplayNames() {
        Descriptor<?> off = TisPluginFormTestSupport.descriptorOf(SecondaryMetricOff.class);
        Assert.assertEquals("off 子类的 displayName 必须是 Descriptor.SWITCH_OFF（\"off\"）——"
                        + "宿主 .json 的 dftVal 按裸字符串匹配它",
                Descriptor.SWITCH_OFF, off.getDisplayName());
        Assert.assertEquals("off 子类的 descriptor id 应是自身类 FQCN",
                SecondaryMetricOff.class.getName(), off.getId());
        Assert.assertEquals("off descriptor 的泛型实参应是抽象父类",
                AbstractSecondaryMetricConfig.class, off.getT());

        Descriptor<?> enabled = TisPluginFormTestSupport.descriptorOf(SecondaryMetricConfig.class);
        Assert.assertEquals("启用实现的 displayName 应保持 \"Secondary Metric Config\"",
                "Secondary Metric Config", enabled.getDisplayName());
        Assert.assertEquals("启用实现的 descriptor id 应是自身类 FQCN",
                SecondaryMetricConfig.class.getName(), enabled.getId());
        Assert.assertEquals("启用实现 descriptor 的泛型实参也应是抽象父类",
                AbstractSecondaryMetricConfig.class, enabled.getT());

        Assert.assertNotEquals("两个子类的 descriptor id 不得相同（descriptor 被提到父类会撞号）",
                off.getId(), enabled.getId());
    }

    /**
     * 宿主 {@code MetricConfig.secondaryMetric} 的声明类型必须是抽象父类、默认值必须是 off。
     *
     * <p>默认值若仍是「启用」，它与 {@code valueVariable} 的 {@code Validator.require} 组合会造出
     * <b>一个过不了校验的缺省值</b>：用户没碰这个字段也会被拦在保存之外。
     */
    @Test
    public void hostFieldIsAbstractParentAndDefaultsOff() throws Exception {
        Field field = MetricConfig.class.getField("secondaryMetric");
        Assert.assertEquals("MetricConfig.secondaryMetric 的声明类型应为抽象父类"
                        + "（改成具体子类后 off 选项在类型层面不可表示）",
                AbstractSecondaryMetricConfig.class, field.getType());
        Assert.assertTrue("MetricConfig.secondaryMetric 的 Java 初值应为 off 子类",
                field.get(new MetricConfig()) instanceof SecondaryMetricOff);
        Assert.assertTrue("宿主字段应带 Validator.require（多态化后它恒非空）",
                Arrays.asList(field.getAnnotation(FormField.class).validate()).contains(Validator.require));

        JSONObject resource = TisPluginFormTestSupport.formResource(MetricConfig.class.getName());
        Assert.assertNotNull("MetricConfig.json 应存在", resource);
        JSONObject secondary = resource.getJSONObject("secondaryMetric");
        Assert.assertNotNull("MetricConfig.json 应有 secondaryMetric 条目", secondary);
        Assert.assertEquals("MetricConfig.secondaryMetric 的默认值应为 off（与 Validator.require 配套）",
                Descriptor.SWITCH_OFF, secondary.getString("dftVal"));
    }

    /** 本类自身或祖先链上声明的 {@code @FormField} 字段（按字段名索引） */
    private static Map<String, FormField> formFieldsOf(Class<?> clazz) {
        List<Field> fields = new ArrayList<>();
        for (Class<?> c = clazz; c != null && c != Object.class; c = c.getSuperclass()) {
            fields.addAll(Arrays.asList(c.getDeclaredFields()));
        }
        return fields.stream()
                .filter(f -> f.getAnnotation(FormField.class) != null)
                .collect(Collectors.toMap(Field::getName, f -> f.getAnnotation(FormField.class)));
    }
}
