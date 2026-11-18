package com.qlangtech.tis.plugin.ontology.impl.action;

import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.plugin.IPluginStore;
import com.qlangtech.tis.plugin.MultiDescribleElementSetSelector;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.CreateObjectRule;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.DeleteObjectRule;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.FunctionBackedRule;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.ModifyObjectRule;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.RuleGroup;
import com.qlangtech.tis.plugin.ontology.impl.action.type.ActionType;
import com.qlangtech.tis.plugin.ontology.impl.action.type.FunctionActionType;
import com.qlangtech.tis.plugin.ontology.impl.action.type.ObjectActionType;
import com.qlangtech.tis.plugin.ontology.workshop.TisPluginFormTestSupport;
import com.qlangtech.tis.util.XStream2;
import com.thoughtworks.xstream.io.xml.XppDriver;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * 「一个 Action 可以组合多条规则」改造的回归测试。
 *
 * <p>本轮把 {@code ActionRuleSetter.actionType}（单条）换成
 * {@code List<ActionType> actionTypes}（{@code MULTI_DESCRIBLE_PLUGIN}）。改动面上几乎每一处
 * <b>都没有编译期信号</b>：
 *
 * <ol>
 *   <li>元素类必须实现 {@code MultiDescribleElement} 且恰好一个 identity 字段 —— 只在表单初始化
 *       （{@code PropertyType} 构造期）才抛，编译照过；</li>
 *   <li>{@code colsConfig()} 里的列名不是真字段、或没有 clickable 列 —— 前者静默空白列，
 *       后者 {@code getExtractProps} 直接抛；</li>
 *   <li>存量单规则 XML 的迁移依赖 {@code readResolve} —— 字段忘了去 transient 就永远迁移不到；</li>
 *   <li>规则组合校验是纯业务规则，写漏一条就是「能存下来但执行时语义错」。</li>
 * </ol>
 *
 * <p>本类不依赖 TIS 运行期（不需要 {@code --add-opens}）：{@code readResolve} 走真实的
 * {@link XStream2}（{@code RobustReflectionConverter} 是实际生效的转换器），
 * 其余靠反射与 classloader 读资源。
 */
public class TestActionRulesMulti {

    private static final String TYPE_PKG = "com.qlangtech.tis.plugin.ontology.impl.action.type";

    // ===================================================================
    // 1. ActionRuleSetter 字段契约
    // ===================================================================

    /**
     * {@code actionTypes} 必须是 {@code MULTI_DESCRIBLE_PLUGIN}，且 {@code desClazz} 指到
     * {@link ActionType}；不能再叠 {@code @SubForm}。
     */
    @Test
    public void actionTypesFieldIsMultiDescriblePlugin() throws Exception {
        Field field = ActionRuleSetter.class.getDeclaredField("actionTypes");
        FormField formField = field.getAnnotation(FormField.class);
        Assert.assertNotNull("actionTypes 必须有 @FormField", formField);
        Assert.assertEquals("一个 Action 可以组合多条规则，字段类型必须是嵌套子插件列表",
                FormFieldType.MULTI_DESCRIBLE_PLUGIN, formField.type());
        Assert.assertEquals("元素类必须是 ActionType（PropertyType 构造期会校验泛型实参一致）",
                ActionType.class, formField.desClazz());
        Assert.assertEquals("字段声明必须是 List", List.class, field.getType());
        Assert.assertNull("type-13 的嵌套形态完全由 desClazz 决定，叠 @SubForm 会让表单初始化走错分支",
                field.getAnnotation(com.qlangtech.tis.plugin.annotation.SubForm.class));
    }



    /**
     * {@code getActionTypes()} 永不返回 null —— 下游 {@code resolveLinkTargets} /
     * {@code getLiteria} 等都直接遍历它。
     */
    @Test
    public void getActionTypesNeverReturnsNull() {
        ActionRuleSetter setter = new ActionRuleSetter();
        Assert.assertNotNull(setter.getActionTypes());
        Assert.assertTrue(setter.getActionTypes().isEmpty());
    }

    // ===================================================================
    // 2. 存量数据迁移
    // ===================================================================



    /**
     * 新格式（{@code actionTypes} 列表）反序列化后不能反过来被迁移逻辑覆盖，且列表顺序必须保住
     * —— 列表位置即执行顺序。
     */
    @Test
    public void newFormatXmlKeepsListOrder() {
        ActionRuleSetter setter = new ActionRuleSetter();
        // 必须是真正的 ArrayList：Arrays.asList 返回的是 Arrays$ArrayList，
        // XStream 会按反射逐字段序列化它（连 AbstractList.modCount 一起），
        // 在 JDK 17 上直接 InaccessibleObjectException
        setter.actionTypes = new java.util.ArrayList<>(Arrays.asList(
                objectRule(new CreateObjectRule(), "先创建", "Order"),
                objectRule(new ModifyObjectRule(), "后修改", "Order"),
                objectRule(new DeleteObjectRule(), "最后删除", "Order")));

        XStream2 xstream = new XStream2(new XppDriver());
        ActionRuleSetter restored = (ActionRuleSetter) xstream.fromXML(xstream.toXML(setter));

        List<ActionType> rules = restored.getActionTypes();
        Assert.assertEquals("三条规则都要留住", 3, rules.size());
        Assert.assertEquals("列表顺序即执行顺序，不能被重排",
                Arrays.asList("先创建", "后修改", "最后删除"),
                rules.stream().map(ActionType::identityValue).toList());
        Assert.assertTrue("第一条应是创建", rules.get(0).getRule() instanceof CreateObjectRule);
        Assert.assertTrue("第三条应是删除", rules.get(2).getRule() instanceof DeleteObjectRule);
    }

    // ===================================================================
    // 3. 规则组合校验
    // ===================================================================

    @Test
    public void emptyRulesAreRejected() {
        Assert.assertTrue("空列表必须报错（零规则 Action 没有意义）",
                RuleCombinationValidator.validate(Collections.emptyList()).isPresent());
        Assert.assertTrue(RuleCombinationValidator.validate(null).isPresent());
    }

    @Test
    public void singleRuleIsAlwaysValid() {
        for (String name : Arrays.asList("CreateObjectRule", "ModifyObjectRule", "DeleteObjectRule")) {
            ActionType rule = switch (name) {
                case "CreateObjectRule" -> objectRule(new CreateObjectRule(), name, "Order");
                case "ModifyObjectRule" -> objectRule(new ModifyObjectRule(), name, "Order");
                default -> objectRule(new DeleteObjectRule(), name, "Order");
            };
            Assert.assertTrue("单条规则（含单独删除）必须合法：" + name,
                    RuleCombinationValidator.validate(List.of(rule)).isEmpty());
        }
    }

    /**
     * Palantir: "Objects cannot be modified before they are added."
     */
    @Test
    public void modifyBeforeCreateIsRejected() {
        Optional<String> err = RuleCombinationValidator.validate(Arrays.asList(
                objectRule(new ModifyObjectRule(), "改", "Order"),
                objectRule(new CreateObjectRule(), "建", "Order")));
        Assert.assertTrue("修改排在创建之前必须被拦下", err.isPresent());
    }

    /**
     * Palantir: "Objects cannot be deleted before they are added or modified."
     */
    @Test
    public void deleteBeforeCreateOrModifyIsRejected() {
        Assert.assertTrue("删除排在创建之前必须被拦下",
                RuleCombinationValidator.validate(Arrays.asList(
                        objectRule(new DeleteObjectRule(), "删", "Order"),
                        objectRule(new CreateObjectRule(), "建", "Order"))).isPresent());

        Assert.assertTrue("删除排在修改之前必须被拦下",
                RuleCombinationValidator.validate(Arrays.asList(
                        objectRule(new DeleteObjectRule(), "删", "Order"),
                        objectRule(new ModifyObjectRule(), "改", "Order"))).isPresent());
    }

    /**
     * 顺序检查必须是「先收集全部出现位置再比较」：删除在前、创建在后时，扫到删除那一刻
     * 创建位置还未知 —— 边扫边比会漏判。
     */
    @Test
    public void deleteFirstIsCaughtRegardlessOfScanOrder() {
        Optional<String> err = RuleCombinationValidator.validate(Arrays.asList(
                objectRule(new DeleteObjectRule(), "删", "Order"),
                objectRule(new CreateObjectRule(), "建", "Order"),
                objectRule(new ModifyObjectRule(), "改", "Order")));
        Assert.assertTrue("删除在最前、创建在中间的组合必须被拦下（边扫边比会漏掉）", err.isPresent());
    }

    @Test
    public void createThenModifyThenDeleteIsValid() {
        Assert.assertTrue("创建 → 修改 → 删除 是合法顺序",
                RuleCombinationValidator.validate(Arrays.asList(
                        objectRule(new CreateObjectRule(), "建", "Order"),
                        objectRule(new ModifyObjectRule(), "改", "Order"),
                        objectRule(new DeleteObjectRule(), "删", "Order"))).isEmpty());
    }

    /**
     * Palantir: "Objects cannot be created twice in one form submission."
     */
    @Test
    public void duplicateCreateIsRejected() {
        Optional<String> err = RuleCombinationValidator.validate(Arrays.asList(
                objectRule(new CreateObjectRule(), "建一", "Order"),
                objectRule(new CreateObjectRule(), "建二", "Order")));
        Assert.assertTrue("同一对象类型不能在一次提交里创建两次", err.isPresent());
    }

    /**
     * 顺序约束只在<b>同一对象类型</b>上成立：不同对象类型各自「修改前的状态」互不相干。
     */
    @Test
    public void orderCheckIsPerObjectType() {
        Assert.assertTrue("不同对象类型之间不存在先后约束",
                RuleCombinationValidator.validate(Arrays.asList(
                        objectRule(new DeleteObjectRule(), "删订单", "Order"),
                        objectRule(new CreateObjectRule(), "建客户", "Customer"))).isEmpty());
    }

    /**
     * 函数规则是唯一例外：存在时不允许配置任何其它规则。
     */
    @Test
    public void functionRuleMustBeExclusive() {
        FunctionActionType function = new FunctionActionType();
        function.ruleName = "函数";
        function.functionRule = new FunctionBackedRule();

        Assert.assertTrue("函数规则单独使用是合法的",
                RuleCombinationValidator.validate(List.of(function)).isEmpty());

        Assert.assertTrue("函数规则 + 对象规则必须被拦下",
                RuleCombinationValidator.validate(Arrays.asList(
                        function, objectRule(new CreateObjectRule(), "建", "Order"))).isPresent());

        // 顺序无关：函数规则排在后面同样非法
        Assert.assertTrue("函数规则排在列表末尾也必须被拦下",
                RuleCombinationValidator.validate(Arrays.asList(
                        objectRule(new CreateObjectRule(), "建", "Order"), function)).isPresent());
    }

    // ===================================================================
    // 4. ActionType 作为 type-13 元素类
    // ===================================================================

    /**
     * {@link ActionType} 必须满足 {@code MultiDescribleElement} 的运行期契约。
     *
     * <p>identity 字段刻意检查「全部字段」而非 declared：identity 声明在抽象基类上，
     * 各子类只是继承。
     */
    @Test
    public void actionTypeHonoursMultiDescribleContract() {
        Assert.assertTrue("ActionType 必须实现 MultiDescribleElement"
                        + "（PropertyType 构造期强制校验，缺了在表单初始化时才抛）",
                IPluginStore.MultiDescribleElement.class.isAssignableFrom(ActionType.class));

        Field identityField = identityFieldOf(ActionType.class);
        Assert.assertEquals("identity 字段名是前端 descVal.vals 的 key，改名前先确认前端",
                ActionType.KEY_RULE_NAME, identityField.getName());
        Assert.assertTrue("identity 字段不得标 Validator.identity —— 该校验器只允许 [A-Z\\da-z_\\-]+，"
                        + "会拒绝「创建工单」这类规则名",
                !Arrays.asList(identityField.getAnnotation(FormField.class).validate())
                        .contains(com.qlangtech.tis.plugin.annotation.Validator.identity));

        ObjectActionType probe = new ObjectActionType();
        probe.ruleName = "创建工单";
        Assert.assertEquals("identityValue() 必须返回 identity 字段的值（前端用它作行 key）",
                "创建工单", probe.identityValue());
    }

    /**
     * 每个具体子类的 descriptor 都必须是 {@code MultiDescribleElementDescriptor}，
     * 且 {@code colsConfig()} 可用、{@code viewStyle()} 为 Table。
     */
    @Test
    public void allActionTypeDescriptorsAreUsable() {
        Set<Class<?>> subclasses = TisPluginFormTestSupport.concreteSubclassesOf(ActionType.class, TYPE_PKG);
        Assert.assertFalse("应扫到 ActionType 的具体子类", subclasses.isEmpty());

        Set<String> formFieldNames = new java.util.TreeSet<>();
        for (Class<?> c = ActionType.class; c != null; c = c.getSuperclass()) {
            for (Field f : c.getDeclaredFields()) {
                if (f.getAnnotation(FormField.class) != null) {
                    formFieldNames.add(f.getName());
                }
            }
        }

        for (Class<?> clazz : subclasses) {
            String name = clazz.getSimpleName();
            Descriptor<?> desc = TisPluginFormTestSupport.descriptorOf(clazz);
            Assert.assertTrue(name + " 的 descriptor 必须继承 MultiDescribleElementDescriptor",
                    desc instanceof MultiDescribleElementSetSelector.MultiDescribleElementDescriptor);

            MultiDescribleElementSetSelector.MultiDescribleElementDescriptor<?> multiDesc =
                    (MultiDescribleElementSetSelector.MultiDescribleElementDescriptor<?>) desc;
            Assert.assertFalse(name + " 的 displayName 不得为空", isBlank(multiDesc.getDisplayName()));
            Assert.assertFalse(name + " 的 shortComment 不得为空（规则列表里只显示它）",
                    isBlank(multiDesc.shortComment()));
            Assert.assertEquals(name + " 应使用表格视图", MultiDescribleElementSetSelector.MultiDescribleElementDescriptor.ViewStyle.Table, multiDesc.viewStyle());

            List<MultiDescribleElementSetSelector.MultiDescribleElementDescriptor.ColConfig> cols = multiDesc.colsConfig();
            Assert.assertFalse(name + " 的 colsConfig() 不得为空", cols.isEmpty());
            boolean clickable = false;
            for (MultiDescribleElementSetSelector.MultiDescribleElementDescriptor.ColConfig col : cols) {
                if (Boolean.TRUE.equals(col.getClickable())) {
                    clickable = true;
                }
                Assert.assertTrue(name + ".colsConfig() 的列「" + col.getTitleKey()
                                + "」不是本类（含父类）上真实存在的 @FormField（写错的列在前端是空白，不报错）",
                        formFieldNames.contains(col.getTitleKey()));
            }
            Assert.assertTrue(name + " 的 colsConfig() 必须至少有一列 clickable"
                    + "（Table 视图下没有 clickable 列时 getExtractProps 直接抛异常）", clickable);
        }
    }

    /**
     * 每个子类都必须能报出规则类别，且 {@code getDisplayName()} 取的就是类别名 ——
     * 它是规则选择下拉里的显示文本。
     */
    @Test
    public void descriptorsReportRuleGroup() {
        for (Class<?> clazz : TisPluginFormTestSupport.concreteSubclassesOf(ActionType.class, TYPE_PKG)) {
            Descriptor<?> desc = TisPluginFormTestSupport.descriptorOf(clazz);
            ActionType.BaseActionTypeDescriptor baseDesc = (ActionType.BaseActionTypeDescriptor) desc;
            RuleGroup group = baseDesc.getRuleGroup();
            Assert.assertNotNull(clazz.getSimpleName() + " 必须报出 RuleGroup", group);
            Assert.assertEquals(clazz.getSimpleName() + " 的 displayName 应取自 RuleGroup",
                    group.getDisplayName(), desc.getDisplayName());
        }
    }

    /**
     * {@code ActionType.json} 的键必须是真实字段名（identity 字段的 label/placeholder 来源）。
     */
    @Test
    public void actionTypeJsonHasNoOrphanKey() {
        JSONObject resource = TisPluginFormTestSupport.formResource(ActionType.class.getName());
        Assert.assertNotNull("ActionType.json 应存在（ruleName 的 label/help 只能写在 json 里）", resource);

        Set<String> formFields = new java.util.TreeSet<>();
        for (Class<?> c = ActionType.class; c != null; c = c.getSuperclass()) {
            for (Field f : c.getDeclaredFields()) {
                if (f.getAnnotation(FormField.class) != null) {
                    formFields.add(f.getName());
                }
            }
        }
        for (String key : resource.keySet()) {
            Assert.assertTrue("ActionType.json 的键「" + key + "」在 Java 侧没有对应字段，现有字段：" + formFields,
                    formFields.contains(key));
        }
    }

    // ===================================================================
    //  helpers
    // ===================================================================

    private static ObjectActionType objectRule(com.qlangtech.tis.plugin.ontology.impl.action.rule.OntologyActionRule rule,
                                               String ruleName, String objectType) {
        ObjectActionType actionType = new ObjectActionType();
        actionType.ruleName = ruleName;
        actionType.objectRule = rule;
        actionType.objectTypeReference = objectType;
        return actionType;
    }

    private static Field identityFieldOf(Class<?> clazz) {
        for (Class<?> c = clazz; c != null; c = c.getSuperclass()) {
            for (Field f : c.getDeclaredFields()) {
                FormField ff = f.getAnnotation(FormField.class);
                if (ff != null && ff.identity()) {
                    return f;
                }
            }
        }
        throw new AssertionError(clazz.getSimpleName() + " 上找不到 @FormField(identity = true) 字段");
    }

    private static void setLegacyActionType(ActionRuleSetter setter, ActionType value) {
        try {
            Field f = ActionRuleSetter.class.getDeclaredField("actionType");
            f.setAccessible(true);
            f.set(setter, value);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("写入旧字段 actionType 失败", e);
        }
    }

    private static ActionType getLegacyActionType(ActionRuleSetter setter) {
        try {
            Field f = ActionRuleSetter.class.getDeclaredField("actionType");
            f.setAccessible(true);
            return (ActionType) f.get(setter);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("读取旧字段 actionType 失败", e);
        }
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}
