package com.qlangtech.tis.plugin.ontology.impl.action;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.OneStepOfMultiSteps;
import com.qlangtech.tis.plugin.IPluginStore;
import com.qlangtech.tis.plugin.IdentityName;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.HttpHeaderConfig;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.PropertyMappingConfig;
import com.qlangtech.tis.plugin.ontology.workshop.TisPluginFormTestSupport;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Action 多步骤配置链与「结构化子表单元素类」的接线回归测试。
 *
 * <p>本轮做了两件互相独立、且都<b>没有编译期保护</b>的事：
 *
 * <ol>
 *   <li><b>删 ActionRules，Step4 换成结构化的提交条件</b> —— 步骤数从「声明 4 / 断言 3」的
 *       自相矛盾里解开（{@code OntologyAction#setSteps} 改成从
 *       {@code getStepDescriptionList()} 推导）；旧的 Step4 与 Step2 步骤描述<b>都叫 "Rules"</b>，
 *       重名不会有任何报错，只会让用户在向导里看到两个一模一样的步骤标题</li>
 *   <li><b>四处「手写 JSON / 文本」的字段改成 {@code MULTI_DESCRIBLE_PLUGIN} 子表单</b> ——
 *       元素类的契约（identity 字段、clickable 列、json 覆盖）全部只在运行期生效</li>
 * </ol>
 *
 * <p>本类全部断言<b>不依赖 TIS 运行期</b>（不需要 {@code --add-opens}）：类清单来自编译期生成的
 * sezpoz 索引，资源靠 classloader 读，descriptor 直接 {@code newInstance}。这样在默认 surefire
 * 配置下就能真跑，不会因为环境问题整类变成「一直绿但什么都没查」。
 */
public class TestActionStepChain {

    private static final String ACTION_PKG = "com.qlangtech.tis.plugin.ontology.impl.action.";

    /** 被删除的旧步骤类，任何一处复辟都说明改造被回退了 */
    private static final String REMOVED_ACTION_RULES = ACTION_PKG + "ActionRules";

    /** 本轮的四个结构化元素类 */
    private static final List<Class<?>> ELEMENT_CLASSES = Arrays.asList(
            ActionParameterConfig.class, SubmissionCriterion.class,
            PropertyMappingConfig.class, HttpHeaderConfig.class);

    // ===================================================================
    // 1. ActionRules 不得复辟
    // ===================================================================

    /**
     * {@code ActionRules} 必须从 classpath 上彻底消失。
     *
     * <p>三种残留各查一遍，因为它们来自三个不同的地方：{@code Class.forName} 查编译产物、
     * {@code formResource} 查被打进 jar 的 json 资源、{@code .class} 文件查增量编译没清干净的
     * {@code target/classes}（sezpoz 索引里若还留着旧条目，运行期会照旧注册出一个 descriptor）。
     */
    @Test
    public void actionRulesMustNotSurvive() {
        try {
            Class<?> stale = Class.forName(REMOVED_ACTION_RULES);
            Assert.fail("ActionRules 已被 ActionSubmissionCriteria 取代，不得再出现在 classpath 上"
                    + "（实际加载到 " + stale.getName() + "，多半是 target 下的陈旧编译产物或 sezpoz 索引残留）");
        } catch (ClassNotFoundException expected) {
            // 正是期望的结果
        }

        Assert.assertNull("ActionRules.json 已删除，不应还能从 classpath 读到",
                TisPluginFormTestSupport.formResource(REMOVED_ACTION_RULES));

        // 步骤清单里也不能再出现旧的 Steps 类名
        for (OneStepOfMultiSteps.BasicDesc desc : stepDescriptionList()) {
            Assert.assertFalse("步骤清单里不应再有 ActionRules.Desc",
                    REMOVED_ACTION_RULES.equals(desc.getClass().getName()));
        }
    }

    // ===================================================================
    // 2. 步骤链自洽
    // ===================================================================

    /**
     * 步骤清单恰为 Metadata → RuleSetter → Parameters → SubmissionCriteria，
     * 且每步的 {@code getStep()} 依次落在 Step1..Step4。
     */
    @Test
    public void stepChainIsWellFormed() {
        List<OneStepOfMultiSteps.BasicDesc> descs = stepDescriptionList();
        Assert.assertEquals("Action 步骤链应恰为 4 步",
                Arrays.asList(ActionMetadata.Desc.class, ActionRuleSetter.Desc.class,
                        ActionParameters.Desc.class, ActionSubmissionCriteria.Desc.class),
                new ArrayList<>(descs.stream().map(Object::getClass).toList()));

        for (int i = 0; i < descs.size(); i++) {
            Assert.assertEquals("第 " + (i + 1) + " 步的 getStep() 应是 Step" + (i + 1)
                            + "（步骤按下标落盘，槽位对不上就会覆盖别的步骤或者读不到）",
                    i, descs.get(i).getStep().getStepIndex());
        }
    }

    /**
     * <b>任两步的步骤标题互不相同。</b>
     *
     * <p>专盯旧实现的双 "Rules" 重名：{@code ActionRuleSetter.Desc} 与已删的
     * {@code ActionRules.Desc} 都返回 {@code "Rules"}，前端步骤条上会出现两个同名步骤，
     * 没有任何报错，只能靠人眼发现。
     */
    @Test
    public void stepDescriptionsAreUnique() {
        Map<String, String> titleToClass = new LinkedHashMap<>();
        for (OneStepOfMultiSteps.BasicDesc desc : stepDescriptionList()) {
            String title = desc.getStepDescription();
            Assert.assertNotNull("步骤标题不得为 null", title);
            String previous = titleToClass.put(title, desc.getClass().getSimpleName());
            Assert.assertNull("步骤标题「" + title + "」被 " + previous + " 与 "
                            + desc.getClass().getSimpleName() + " 共用（步骤名重复不会有任何报错）",
                    previous);
        }
    }

    // ===================================================================
    // 3. 链的传递闭包
    // ===================================================================

    /**
     * {@code isFinalStep()} 为真 ⟺ {@code nextPluginDesc} 为空；非末步则下一步的
     * {@code getStep()} 必须恰好是当前 +1。
     *
     * <p>{@code isFinalStep()} 的默认实现就是「nextPluginDesc 为空」，但各步骤全都是显式覆写的，
     * 两者可以写出不一致（返回了下一步却自称末步，或反之）。
     */
    @Test
    public void nextStepChainIsConsistent() {
        List<OneStepOfMultiSteps.BasicDesc> descs = stepDescriptionList();
        for (OneStepOfMultiSteps.BasicDesc desc : descs) {
            boolean hasNext = desc.nextPluginDesc(null).isPresent();
            Assert.assertEquals(desc.getClass().getSimpleName()
                            + "：isFinalStep() 与 nextPluginDesc() 必须互为反命题",
                    hasNext, !desc.isFinalStep());
        }

        // 非末步必须接上清单里的下一步，且中间不能断
        for (int i = 0; i < descs.size() - 1; i++) {
            OneStepOfMultiSteps.BasicDesc current = descs.get(i);
            OneStepOfMultiSteps.BasicDesc next = current.nextPluginDesc(null).orElseThrow(
                    () -> new AssertionError(current.getClass().getSimpleName() + " 不是末步却给不出下一步"));
            Assert.assertEquals(current.getClass().getSimpleName() + " 的下一步应是清单里的第 "
                            + (i + 2) + " 步", descs.get(i + 1).getClass(), next.getClass());
        }
        Assert.assertTrue("最后一步必须是末步", descs.get(descs.size() - 1).isFinalStep());
    }

    // ===================================================================
    // 4. 步骤数断言与声明同源
    // ===================================================================

    /**
     * {@code OntologyAction#setSteps} 的期望长度不再写死，而是从
     * {@code getStepDescriptionList().size()} 取。这里钉住这个等式成立的前提：
     * 声明必须是 4 步。
     *
     * <p>旧实现写死 {@code FIXED_ACTION_STEPS_LENGTH = 3} 而声明是 4 步，
     * 保存时必然抛 {@code IllegalStateException}——两处各说各话是这类 bug 的根因。
     */
    @Test
    public void stepCountIsFourAndDerivedFromSameSource() {
        Assert.assertEquals("DefaultDesc 声明的步骤数变了；setSteps 会跟着变，"
                + "但如果前端仍只提交 3 步，这里是唯一能提前发现的地方", 4, stepDescriptionList().size());
    }

    // ===================================================================
    // 5. 元素类契约
    // ===================================================================

    /**
     * 四个元素类必须满足 {@code MULTI_DESCRIBLE_PLUGIN} 的全部运行期契约：
     * identity 唯一、clickable 列存在、列名都是真字段、displayName/shortComment 非空。
     */
    @Test
    public void elementClassesHonourMultiDescribleContract() {
        for (Class<?> clazz : ELEMENT_CLASSES) {
            String name = clazz.getSimpleName();

            Assert.assertTrue(name + " 必须实现 IPluginStore.MultiDescribleElement"
                            + "（PropertyType 构造期强制校验，缺了在表单初始化时才抛）",
                    IPluginStore.MultiDescribleElement.class.isAssignableFrom(clazz));
            Assert.assertTrue(name + " 必须实现 IdentityName（MultiDescribleElement 的父接口）",
                    IdentityName.class.isAssignableFrom(clazz));

            // 恰好一个 identity 字段
            List<Field> identityFields = new ArrayList<>();
            for (Field f : clazz.getDeclaredFields()) {
                FormField ff = f.getAnnotation(FormField.class);
                if (ff != null && ff.identity()) {
                    identityFields.add(f);
                }
            }
            Assert.assertEquals(name + " 必须恰好有一个 @FormField(identity = true) 字段"
                    + "（框架用它的「值」作行记录的 key，多一个就撞号）",
                    1, identityFields.size());

            Field identityField = identityFields.get(0);
            Assert.assertTrue(name + "." + identityField.getName()
                            + " 是 identity 字段，不得标 Validator.identity —— 该校验器只允许"
                            + " [A-Z\\da-z_\\-]+，会拒绝「金额下限」这类天然是中文的行名",
                    !Arrays.asList(identityField.getAnnotation(FormField.class).validate())
                            .contains(Validator.identity));

            // identityValue() 返回的必须就是那个 identity 字段
            assertIdentityValueReturnsIdentityField(clazz, identityField);

            // descriptor 契约
            Descriptor<?> desc = descriptorOf(clazz);
            Assert.assertNotNull(name + " 必须有可实例化的嵌套 descriptor", desc);
            Assert.assertFalse(name + " 的 descriptor.getDisplayName() 不得为空",
                    isBlank(desc.getDisplayName()));
            Assert.assertFalse(name + " 的 descriptor.shortComment() 不得为空"
                            + "（列表里只显示短注释，空着等于没说明）",
                    isBlank(((IPluginStore.MultiDescribleElementDescriptor<?>) desc).shortComment()));

            assertColsConfigIsUsable(clazz, (IPluginStore.MultiDescribleElementDescriptor<?>) desc);
        }
    }

    /**
     * {@code colsConfig()} 必须至少有一列 {@code clickable}，且每个 {@code titleKey}
     * 都是该元素类上真实存在的 {@code @FormField} 字段名。
     *
     * <p>两个都是静默失效：没有 clickable 列时 {@code getExtractProps} 直接抛异常（整个表单
     * 打不开）；列名写错时前端那一列取不到值，表现为空白列，不报错。
     */
    private static void assertColsConfigIsUsable(Class<?> clazz,
                                                 IPluginStore.MultiDescribleElementDescriptor<?> desc) {
        String name = clazz.getSimpleName();
        List<IPluginStore.MultiDescribleElementDescriptor.ColConfig> cols = desc.colsConfig();
        Assert.assertNotNull(name + ".colsConfig() 不得为 null", cols);
        Assert.assertFalse(name + ".colsConfig() 不得为空", cols.isEmpty());

        Set<String> formFieldNames = fieldNamesOf(clazz);
        boolean anyClickable = false;
        for (IPluginStore.MultiDescribleElementDescriptor.ColConfig col : cols) {
            if (Boolean.TRUE.equals(col.getClickable())) {
                anyClickable = true;
            }
            Assert.assertTrue(name + ".colsConfig() 的列「" + col.getTitleKey()
                            + "」不是本类上的 @FormField 字段（写错的列在前端是空白，不报错）",
                    formFieldNames.contains(col.getTitleKey()));
        }
        Assert.assertTrue(name + ".colsConfig() 必须至少有一列 clickable"
                + "（Table 视图下没有 clickable 列时 getExtractProps 直接抛异常）", anyClickable);
    }

    // ===================================================================
    // 6. json 与字段双向覆盖
    // ===================================================================

    /**
     * 每个元素类的 {@code .json} 键集与它的 {@code @FormField} 字段名集<b>双向相等</b>。
     *
     * <p>漏键 = 该字段 label/help 全空（表单照常渲染）；多键 = 有人删了字段忘了删 json，
     * 或者干脆写错了名字。两个方向都不报错。
     *
     * <p>顺带校验 json 里 enum 数组的每个 {@code val} 都能在该字段的枚举类型上找到同名常量：
     * {@code resolveEnumLabel} 只回 {@code e.name()}，中文标签必须写在 json 里，
     * 而 {@code val} 拼错一个字母就是「选项点了没反应」。
     */
    @Test
    public void jsonCoversFormFieldsBothWays() {
        for (Class<?> clazz : ELEMENT_CLASSES) {
            String name = clazz.getSimpleName();
            JSONObject resource = TisPluginFormTestSupport.formResource(clazz.getName());
            Assert.assertNotNull(name + " 声明了 @FormField，必须有同名 .json", resource);

            Set<String> formFields = fieldNamesOf(clazz);
            Assert.assertEquals(name + " 的 .json 键集与 @FormField 字段名集必须双向相等"
                            + "（漏键→label/help 静默为空；多键→删字段忘了删 json）",
                    formFields, new TreeSet<>(resource.keySet()));

            for (Field f : clazz.getDeclaredFields()) {
                if (f.getAnnotation(FormField.class) == null) {
                    continue;
                }
                // enum 的取值有两种写法：内联数组（中文标签），或一个 Groovy 脚本字符串
                // （`... :uncache_true`，运行期求值）。只有前者能在这里静态核对 val 拼写，
                // 后者刻意跳过 —— 对字符串调 getJSONArray 会直接抛解析异常。
                Object enumVal = resource.getJSONObject(f.getName()).get("enum");
                if (!(enumVal instanceof JSONArray) || !f.getType().isEnum()) {
                    continue;
                }
                JSONArray enumOpts = (JSONArray) enumVal;
                Set<String> constants = new TreeSet<>();
                for (Object c : f.getType().getEnumConstants()) {
                    constants.add(((Enum<?>) c).name());
                }
                for (int i = 0; i < enumOpts.size(); i++) {
                    String val = enumOpts.getJSONObject(i).getString("val");
                    Assert.assertTrue(name + ".json 的 " + f.getName() + " 选项 val=\"" + val
                                    + "\" 在该枚举上找不到同名常量（拼错即「点了没反应」）",
                            constants.contains(val));
                }
            }
        }
    }

    // ===================================================================
    // 7. ActionRuleSetter.json 无孤儿键
    // ===================================================================

    /**
     * {@code ActionRuleSetter.json} 的键集必须是 Java 字段名集的子集。
     *
     * <p>专盯 {@code interfaceType}：它的 Java 字段早已被注释掉，json 条目却一直留着，
     * 前端于是照旧渲染出一个「接口操作」下拉——选了没有任何效果。
     */
    @Test
    public void actionRuleSetterJsonHasNoOrphanKey() {
        JSONObject resource = TisPluginFormTestSupport.formResource(ActionRuleSetter.class.getName());
        Assert.assertNotNull("ActionRuleSetter.json 应存在", resource);

        Set<String> formFields = fieldNamesOf(ActionRuleSetter.class);
        for (String key : resource.keySet()) {
            Assert.assertTrue("ActionRuleSetter.json 的键「" + key + "」在 Java 侧没有对应字段"
                            + "（孤儿键会被照旧渲染成一个选了没效果的表单项），现有字段：" + formFields,
                    formFields.contains(key));
        }
    }

    // ===================================================================
    // 8. 一元操作符
    // ===================================================================

    /**
     * 只有「为空 / 不为空」不需要比较值。
     *
     * <p>{@code isNullary()} 是提交时「填了比较值就报错」那条行内校验的判据，多一个少一个
     * 都会让该拦的没拦或者把合法输入拦下。
     */
    @Test
    public void onlyNullChecksAreNullary() {
        Set<SubmissionCriterion.ComparisonOperator> expectedNullary = new TreeSet<>(Arrays.asList(
                SubmissionCriterion.ComparisonOperator.IS_NULL,
                SubmissionCriterion.ComparisonOperator.IS_NOT_NULL));

        Set<SubmissionCriterion.ComparisonOperator> actualNullary = new TreeSet<>();
        for (SubmissionCriterion.ComparisonOperator op : SubmissionCriterion.ComparisonOperator.values()) {
            if (op.isNullary()) {
                actualNullary.add(op);
            }
        }
        Assert.assertEquals("只有「为空 / 不为空」是一元操作符", expectedNullary, actualNullary);
    }

    // ===================================================================
    //  helpers
    // ===================================================================

    private static List<OneStepOfMultiSteps.BasicDesc> stepDescriptionList() {
        List<OneStepOfMultiSteps.BasicDesc> descs =
                new DefaultOntologyAction.DefaultDesc().getStepDescriptionList();
        Assert.assertNotNull("getStepDescriptionList() 不得为 null"
                + "（它是前端步骤清单的唯一数据源，空列表直接抛 IllegalStateException）", descs);
        return descs;
    }

    private static Descriptor<?> descriptorOf(Class<?> pluginClass) {
        return TisPluginFormTestSupport.descriptorOf(pluginClass);
    }

    private static Set<String> fieldNamesOf(Class<?> clazz) {
        Set<String> names = new TreeSet<>();
        for (Field f : clazz.getDeclaredFields()) {
            if (f.getAnnotation(FormField.class) != null) {
                names.add(f.getName());
            }
        }
        Assert.assertFalse(clazz.getSimpleName() + " 上没有任何 @FormField 字段", names.isEmpty());
        return names;
    }

    private static void assertIdentityValueReturnsIdentityField(Class<?> clazz, Field identityField) {
        Method identityValue;
        try {
            identityValue = clazz.getMethod("identityValue");
        } catch (NoSuchMethodException e) {
            throw new AssertionError("MultiDescribleElement 必须实现 identityValue()，但 "
                    + clazz.getSimpleName() + " 上没有", e);
        }

        Object instance;
        try {
            instance = clazz.getDeclaredConstructor().newInstance();
        } catch (Throwable t) {
            throw new AssertionError(clazz.getSimpleName() + " 无法无参实例化", t);
        }

        String marker = "identityProbe" + clazz.getSimpleName();
        try {
            identityField.setAccessible(true);
            Class<?> fieldType = identityField.getType();
            if (fieldType == String.class) {
                identityField.set(instance, marker);
                Assert.assertEquals(clazz.getSimpleName() + ".identityValue() 必须返回 identity 字段 "
                                + identityField.getName() + " 的值（前端用它作行记录的 key）",
                        marker, identityValue.invoke(instance));
            } else {
                // 非 String 的 identity 字段本工程没有先例，退化为「方法存在且类型可赋值」的弱断言
                Assert.assertTrue(clazz.getSimpleName() + ".identityValue() 的返回类型应能承载 "
                                + identityField.getType().getName(),
                        identityValue.getReturnType().isAssignableFrom(fieldType)
                                || fieldType.isAssignableFrom(identityValue.getReturnType()));
            }
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("调用 " + clazz.getSimpleName() + ".identityValue() 失败", e);
        }
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}
