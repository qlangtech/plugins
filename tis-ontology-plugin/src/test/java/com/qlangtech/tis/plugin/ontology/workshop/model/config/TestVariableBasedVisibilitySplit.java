package com.qlangtech.tis.plugin.ontology.workshop.model.config;

import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.manage.common.Option;
import com.qlangtech.tis.manage.common.OptionWithEndType;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.workshop.TisPluginFormTestSupport;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * {@code VariableBasedVisibility} 多态化结构回归：抽象父类 + off / 默认两个子类。
 *
 * <p>与 {@code TestConditionalVisibilitySplit} 同路数，但多钉一条<b>本类专属</b>的线格式契约：
 * {@code expression} 从裸 {@code String} 换成了枚举，而枚举的持久化形态是
 * {@code name()}（XStream）与选项 value（{@code OptionWithEndType#createEnumOptions}）。
 * 常量名一旦改成大写、或顺手实现 {@code IEnumTypeSer} 换 token，都会让
 * 老数据读不回 / 前端回填匹配不上选项 —— 两端都<b>不报错</b>。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/11/11
 */
public class TestVariableBasedVisibilitySplit {

    private static final String PKG = "com.qlangtech.tis.plugin.ontology.workshop.model.config.";

    private static final String OVERLAY_FQCN = "com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopOverlay";

    private static final Comparator<Class<?>> BY_CLASS_NAME = Comparator.comparing((Class<?> c) -> c.getName());

    /** 改造前 expression 的取值集合 —— 枚举常量名必须逐字保留 */
    private static final Set<String> LEGACY_EXPRESSION_TOKENS = new TreeSet<>(Arrays.asList(
            "equals", "notEmpty", "greaterThan", "lessThan", "truthy"));

    @Test
    public void concreteSubclassesAreExactlyOffAndDefault() {
        Set<Class<?>> actual = new TreeSet<>(BY_CLASS_NAME);
        actual.addAll(TisPluginFormTestSupport.concreteSubclassesOf(VariableBasedVisibilityConfig.class, PKG));

        Set<Class<?>> expected = new TreeSet<>(BY_CLASS_NAME);
        expected.add(VariableBasedVisibility.class);
        expected.add(NoneVariableBasedVisibility.class);

        Assert.assertEquals("VariableBasedVisibilityConfig 的具体子类应恰好是 off / 默认实现"
                        + "（少一个子类意味着表单下拉少一项，用户无法选择或关闭）",
                expected, actual);
    }

    @Test
    public void parentAndOffSubclassDeclareNoFormFields() {
        Assert.assertTrue("VariableBasedVisibilityConfig 应是抽象类",
                java.lang.reflect.Modifier.isAbstract(VariableBasedVisibilityConfig.class.getModifiers()));
        Assert.assertEquals("抽象父类不得声明 @FormField —— 字段全在子类，否则与子类 ordinal 撞号",
                0, formFieldsOf(VariableBasedVisibilityConfig.class).size());

        Assert.assertEquals("off 子类应是零字段（「不启用」不需要任何参数）",
                0, formFieldsOf(NoneVariableBasedVisibility.class).size());
        Assert.assertNull("零字段的 off 子类不应有 .json（有 json 意味着有人给它加了字段）",
                TisPluginFormTestSupport.formResource(NoneVariableBasedVisibility.class.getName()));
    }

    @Test
    public void defaultSubclassFieldsAreRequiredAndDescribed() {
        Map<String, FormField> fields = formFieldsOf(VariableBasedVisibility.class);
        Assert.assertEquals("默认实现应恰有 variableId / expression / compareValue 三个表单字段",
                new TreeSet<>(Arrays.asList("variableId", "expression", "compareValue")),
                new TreeSet<>(fields.keySet()));

        for (String name : new String[]{"variableId", "expression"}) {
            Assert.assertTrue(VariableBasedVisibility.class.getSimpleName() + "." + name
                            + " 必须带 Validator.require：多态化后「选默认实现」就是显式启用动作，"
                            + "此时字段不能留空（否则可保存 enabled + 空变量的空壳配置）",
                    Arrays.asList(fields.get(name).validate()).contains(Validator.require));
        }
        Assert.assertFalse("compareValue 不能带 Validator.require —— notEmpty / truthy 用不到比较值",
                Arrays.asList(fields.get("compareValue").validate()).contains(Validator.require));

        Assert.assertEquals("variableId 应是 SELECTABLE（候选变量由本类 descriptor 注册，"
                        + "否则用户只能手填变量名）",
                com.qlangtech.tis.plugin.annotation.FormFieldType.SELECTABLE,
                fields.get("variableId").type());
        Assert.assertEquals("expression 应是 ENUM（固定取值，CLAUDE.md 原则二）",
                com.qlangtech.tis.plugin.annotation.FormFieldType.ENUM,
                fields.get("expression").type());

        JSONObject resource = TisPluginFormTestSupport.formResource(VariableBasedVisibility.class.getName());
        Assert.assertNotNull("默认实现声明了 @FormField，必须有 VariableBasedVisibility.json"
                + "（缺失时表单 label/help 全空且无报错）", resource);
        for (String name : fields.keySet()) {
            Assert.assertNotNull("VariableBasedVisibility.json 缺少字段 " + name + " 的条目",
                    resource.getJSONObject(name));
        }
        // 中文标签由枚举 shortComment() 派生为选项 help；在 json 里写 enum 数组会绕过该派生
        Assert.assertNull("expression 的 json 条目不得带 enum 数组（会绕过 shortComment 派生）",
                resource.getJSONObject("expression").get("enum"));
        Assert.assertEquals("expression 的 dftVal 必须等于 Java 初值的常量名",
                visibilityExpressionDefault(), resource.getJSONObject("expression").getString("dftVal"));
    }

    @Test
    public void descriptorsHaveDistinctIdsAndExpectedDisplayNames() {
        Descriptor<?> off = TisPluginFormTestSupport.descriptorOf(NoneVariableBasedVisibility.class);
        Assert.assertEquals("off 子类的 displayName 必须是 Descriptor.SWITCH_OFF（\"off\"）——"
                        + "宿主 .json 的 dftVal 按裸字符串匹配它",
                Descriptor.SWITCH_OFF, off.getDisplayName());
        Assert.assertEquals("off 子类的 descriptor id 应是自身类 FQCN",
                NoneVariableBasedVisibility.class.getName(), off.getId());
        // 前端 impl 下拉的候选来源是 DescriptorExtensionList<VariableBasedVisibilityConfig>，
        // 由 getT()（沿继承链取 Descriptor 的泛型实参）决定；泛型写错时不会编译报错，
        // 只是这个实现落进别的列表 —— 表现为下拉少一项。
        Assert.assertEquals("off descriptor 的泛型实参应是抽象父类",
                VariableBasedVisibilityConfig.class, off.getT());

        Descriptor<?> enabled = TisPluginFormTestSupport.descriptorOf(VariableBasedVisibility.class);
        Assert.assertEquals("默认实现的 displayName 必须保持 \"Variable Based Visibility\""
                        + "（下拉项标题）",
                "Variable Based Visibility", enabled.getDisplayName());
        Assert.assertEquals("默认实现的 descriptor id 应是自身类 FQCN",
                VariableBasedVisibility.class.getName(), enabled.getId());
        Assert.assertEquals("默认实现 descriptor 的泛型实参也应是抽象父类",
                VariableBasedVisibilityConfig.class, enabled.getT());

        Assert.assertNotEquals("两个子类的 descriptor id 不得相同（descriptor 被提到父类会撞号）",
                off.getId(), enabled.getId());
    }

    /**
     * {@code expression} 的五条线格式契约。
     *
     * <p>枚举把「合法取值」从字符串字面量抬进了类型系统，但也把两条<b>零反馈</b>的隐性契约
     * 留在原处：XStream 按 {@code name()} 落盘/读回；表单选项 value 在未实现
     * {@code IEnumTypeSer} 时同样取 {@code name()}。两条都必须在改造前后保持一致。
     */
    @Test
    public void expressionEnumKeepsLegacyWireTokens() {
        Assert.assertEquals("枚举常量集应恰好覆盖改造前的五个字符串",
                LEGACY_EXPRESSION_TOKENS, new TreeSet<>(Arrays.stream(
                        VariableBasedVisibility.VisibilityExpression.values())
                        .map(Enum::name).collect(Collectors.toList())));

        for (String token : LEGACY_EXPRESSION_TOKENS) {
            // XStream 走 Enum.valueOf：常量名改了这里就抛 IllegalArgumentException
            Assert.assertNotNull("存量 XML 里的 " + token + " 必须仍能读回",
                    VariableBasedVisibility.VisibilityExpression.valueOf(token));
        }

        Set<String> optionValues = new TreeSet<>();
        for (Option opt : OptionWithEndType.createEnumOptions(VariableBasedVisibility.VisibilityExpression.class)) {
            optionValues.add(String.valueOf(opt.getValue()));
            Assert.assertEquals("选项 label 应是常量名：" + opt.getName(),
                    opt.getName(), opt.getValue());
            Assert.assertFalse("中文说明由 shortComment() 派生，不得为空",
                    opt.description() == null || opt.description().trim().isEmpty());
        }
        Assert.assertEquals("表单选项 value 必须仍是旧的五个 token（实现 IEnumTypeSer 换 token 会静默丢配置）",
                LEGACY_EXPRESSION_TOKENS, optionValues);
    }

    @Test
    public void overlayHostDeclaresAbstractTypeAndDefaultsToOff() throws Exception {
        Field field = com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopOverlay.class
                .getField("variableBasedVisibility");
        Assert.assertEquals("WorkshopOverlay.variableBasedVisibility 的声明类型应为抽象父类"
                        + "（改成具体子类后 off 选项在类型层面不可表示）",
                VariableBasedVisibilityConfig.class, field.getType());
        Assert.assertFalse("宿主字段不得加 Validator.require —— 该字段线上可缺省，"
                        + "require 会让老 overlay 在 POST 校验阶段被拦下",
                Arrays.asList(field.getAnnotation(FormField.class).validate()).contains(Validator.require));

        JSONObject resource = TisPluginFormTestSupport.formResource(OVERLAY_FQCN);
        Assert.assertNotNull(OVERLAY_FQCN + ".json 应存在", resource);
        JSONObject visibility = resource.getJSONObject("variableBasedVisibility");
        Assert.assertNotNull(OVERLAY_FQCN + ".json 应有 variableBasedVisibility 条目", visibility);
        Assert.assertEquals("Overlay 的 variableBasedVisibility 默认值应为 off（与 Java 初值配套）",
                Descriptor.SWITCH_OFF, visibility.getString("dftVal"));
    }

    /**
     * XStream 不走构造器与字段初值：存量 overlay XML 没有该元素时，补 off 的唯一机会是
     * {@code readResolve()}。缺了它读出来就是 {@code null}，前端 {@code if (...) } 恒假。
     */
    @Test
    public void overlayReadResolveFillsMissingVisibility() throws Exception {
        com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopOverlay overlay =
                new com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopOverlay();
        overlay.variableBasedVisibility = null;

        Method readResolve = com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopOverlay.class
                .getDeclaredMethod("readResolve");
        readResolve.setAccessible(true);
        readResolve.invoke(overlay);

        Assert.assertTrue("readResolve 缺失字段时应补 NoneVariableBasedVisibility",
                overlay.variableBasedVisibility instanceof NoneVariableBasedVisibility);
    }

    /** Java 初值的常量名，避免测试里再写一遍字面量 */
    private static String visibilityExpressionDefault() {
        return new VariableBasedVisibility().expression.name();
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
