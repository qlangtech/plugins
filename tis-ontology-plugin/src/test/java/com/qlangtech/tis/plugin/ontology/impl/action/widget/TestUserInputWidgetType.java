package com.qlangtech.tis.plugin.ontology.impl.action.widget;

import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.SubForm;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.IPluginStore;
import com.qlangtech.tis.plugin.ontology.impl.action.PrimitiveDataType;
import com.qlangtech.tis.plugin.ontology.impl.action.widget.impl.InputTextWidget;
import com.qlangtech.tis.plugin.ontology.workshop.TisPluginFormTestSupport;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * {@code PrimitiveDataType.widgetType} 从裸枚举 {@link FormFieldType} 改成
 * {@code UserInputWidgetType} 多态插件族之后的接线回归测试。
 *
 * <p>这次改造有一批<b>没有任何编译期保护</b>的接线，全部由本类钉住：
 *
 * <ol>
 *   <li><b>子类 ↔ 枚举 1:1</b> —— 19 个子类的 {@code getFormFieldType()} 必须恰好覆盖
 *       {@link FormFieldType} 的全部常量，不重不漏。漏一个的表现是「界面上那个控件类型
 *       根本没得选」，重复一个则是两个子类争同一类型</li>
 *   <li><b>descriptor 的 clazz</b> —— 每个子类 descriptor 的 {@code getId()} 必须等于
 *       <b>它自己</b>的 FQCN。descriptor 一旦被提到基类上（或写死 {@code super(XxxWidget.class)}），
 *       同族子类的 id 就撞成一个，前端在 impl 下拉里再也分不出选了哪种控件</li>
 *   <li><b>json 的存在性规则</b> —— 带 {@code @FormField} 的子类<b>必须</b>有同名 {@code .json}
 *       且键集与声明字段<b>双向相等</b>；零字段的基类<b>必须没有</b> json</li>
 *   <li><b>默认项接线（跨语言）</b> —— {@code PrimitiveDataType.json} 里 {@code widgetType}
 *       的 {@code dftVal} 必须逐字等于 {@code InputTextWidget} 的 {@code displayName}。
 *       前端拿 {@code dftVal} 去逐项比对 {@code displayName} 选默认项，值写错是静默失效</li>
 *   <li><b>扁平字段不得复辟</b> —— {@code PrimitiveDataType.widgetType} 的类型必须是抽象基类，
 *       不能退回 {@link FormFieldType} 枚举</li>
 * </ol>
 *
 * <p>第 1~5 条不依赖 TIS 运行期（类清单来自编译期 sezpoz 索引，descriptor 直接
 * {@code newInstance}，json 靠 classloader 读），因此在默认 surefire 配置下就能真跑。
 * 只有 {@link #extractPropsCarryFormFieldType()} 需要 TIS，环境不具备时该条整条跳过，
 * 不会连累其余断言。
 */
public class TestUserInputWidgetType {

    /** 19 个具体子类所在的包（基类与 {@code WidgetOptionConfig} 都不在这里） */
    private static final String IMPL_PKG =
            "com.qlangtech.tis.plugin.ontology.impl.action.widget.impl.";

    private static final String FIELD_WIDGET_TYPE = "widgetType";

    /** 下发给前端的渲染派发键，见 {@code UserInputWidgetType.KEY_FORM_FIELD_TYPE} */
    private static final String KEY_FORM_FIELD_TYPE = UserInputWidgetType.KEY_FORM_FIELD_TYPE;

    // ===================================================================
    // 1. 子类 ↔ 枚举 1:1
    // ===================================================================

    /**
     * 19 个子类与 {@link FormFieldType} 的常量集<b>互为双射</b>：不重、不漏、总数相等。
     *
     * <p>三处都单独断言而不是只断言总数：总数对得上仍可能「A 漏了、B 重复了」，
     * 那正好是新增枚举常量时最容易犯的错。
     */
    @Test
    public void subclassesMapToEveryFormFieldTypeExactlyOnce() {
        Set<Class<?>> subs = subclasses();
        Map<FormFieldType, String> seen = new LinkedHashMap<>();
        List<String> clashes = new ArrayList<>();

        for (Class<?> sub : subs) {
            FormFieldType type = instantiate(sub).getFormFieldType();
            String prev = seen.put(type, sub.getSimpleName());
            if (prev != null) {
                clashes.add(type + " 同时被 " + prev + " 与 " + sub.getSimpleName() + " 声明");
            }
        }
        Assert.assertTrue("多个子类映射到同一 FormFieldType（前端会渲染成同一个控件类型）：" + clashes,
                clashes.isEmpty());

        Set<FormFieldType> uncovered = new TreeSet<>(ComparatorHolder.BY_NAME);
        for (FormFieldType t : FormFieldType.values()) {
            if (!seen.containsKey(t)) {
                uncovered.add(t);
            }
        }
        Assert.assertTrue("以下 FormFieldType 常量没有任何子类覆盖（界面上该控件类型选不到）：" + uncovered,
                uncovered.isEmpty());

        Assert.assertEquals("子类数应与 FormFieldType 常量数相等（一一对应）",
                FormFieldType.values().length, seen.size());
        Assert.assertEquals("子类数应与 FormFieldType 常量数相等",
                FormFieldType.values().length, subs.size());
    }

    // ===================================================================
    // 2. descriptor 识别自己的具体子类
    // ===================================================================

    /**
     * 每个子类 descriptor 的 {@code getId()} 必须等于<b>它自己</b>的 FQCN。
     *
     * <p>{@code Descriptor()} 无参构造把 {@code clazz} 置为 {@code getClass().getEnclosingClass()}，
     * 所以只要 descriptor 嵌在具体子类里就自然成立；一旦有人把 descriptor 提到基类上，
     * 19 个子类的 impl 就全部撞成 {@code UserInputWidgetType}，前端拿到的 impl 再也无法区分。
     */
    @Test
    public void descriptorsIdentifyTheirOwnSubclass() {
        for (Class<?> sub : subclasses()) {
            Assert.assertEquals(sub.getSimpleName() + " 的 descriptor.getId() 应等于其自身 FQCN"
                            + "（同族子类撞成同一个 id 时前端无法区分 impl）",
                    sub.getName(), TisPluginFormTestSupport.descriptorOf(sub).getId());
        }
    }

    // ===================================================================
    // 3. json 的存在性规则与键集一致
    // ===================================================================

    /**
     * 带 {@code @FormField}/{@code @SubForm} 的类<b>必须</b>有同名 {@code .json}，
     * 且 json 的顶层键集与声明字段<b>双向相等</b>；零字段的基类<b>必须没有</b> json。
     *
     * <p>三个方向都是静默失效：漏建 json → 字段 label/help 全空但表单照常渲染；
     * json 里少了字段 → 同样只是没提示；多了游离键 → 说明有人删了字段却忘了删 json
     * （或者键名拼错，那个字段的提示其实一直没生效）。
     */
    @Test
    public void formResourceMatchesDeclaredFields() {
        List<Class<?>> all = new ArrayList<>(subclasses());
        // WidgetOptionConfig 在上一层包（是 @SubForm 的元素类，不是 widget 子类），一并钉住
        all.add(WidgetOptionConfig.class);

        for (Class<?> clazz : all) {
            List<Field> fields = formFieldsOf(clazz);
            JSONObject resource = TisPluginFormTestSupport.formResource(clazz.getName());

            if (fields.isEmpty()) {
                Assert.assertNull(clazz.getSimpleName() + " 是零字段类，不应有 .json"
                        + "（有 json 意味着有人往基类里加了字段）", resource);
                continue;
            }

            Assert.assertNotNull(clazz.getSimpleName() + " 声明了表单字段，因此必须有同名 .json"
                    + "（缺失时字段的 label/help 全空且没有任何报错）", resource);

            Set<String> declared = new TreeSet<>();
            for (Field f : fields) {
                declared.add(f.getName());
            }
            Set<String> inJson = new TreeSet<>(resource.keySet());

            Set<String> missing = new TreeSet<>(declared);
            missing.removeAll(inJson);
            Set<String> orphan = new TreeSet<>(inJson);
            orphan.removeAll(declared);

            Assert.assertTrue(clazz.getSimpleName() + ".json 缺少这些字段的条目：" + missing,
                    missing.isEmpty());
            Assert.assertTrue(clazz.getSimpleName() + ".json 里有游离键（字段已删或键名拼错）：" + orphan,
                    orphan.isEmpty());
        }
    }

    /**
     * {@code WidgetOptionConfig} 的 descriptor 必须是
     * {@link com.qlangtech.tis.plugin.IPluginStore.MultiDescribleElementDescriptor}。
     *
     * <p>{@code EnumWidget.options} 走 {@code MULTI_DESCRIBLE_PLUGIN} 收集元素，而这种子列表的展现形态
     * （{@code viewStyle} / {@code colsCfg} / {@code maxHeight}）只在
     * {@code MultiDescribleElementDescriptor#getExtractProps} 里下发 —— 那是个 {@code final} 方法，
     * 只有继承该基类才拿得到。挂一个普通 {@code Descriptor} 编译期不报错、单元素表单也照常渲染，
     * 表现为「子表格列标题/列宽全丢」，没有任何报错。
     *
     * <p>后两条断言需要 TIS 运行期（{@code getExtractProps} 会走属性类型装载），环境不具备时整条跳过，
     * 但 {@code instanceof} 那条无论如何都查。
     */
    @Test
    public void widgetOptionConfigDescriptorIsMultiDescribleElementDescriptor() {
        Descriptor<?> desc = TisPluginFormTestSupport.descriptorOf(WidgetOptionConfig.class);
        Assert.assertTrue("WidgetOptionConfig.DftDescriptor 必须继承 MultiDescribleElementDescriptor"
                        + "（否则子表格拿不到 viewStyle/colsCfg/maxHeight），实际是 "
                        + desc.getClass().getSuperclass().getName(),
                desc instanceof IPluginStore.MultiDescribleElementDescriptor);

        org.junit.Assume.assumeTrue("当前环境未初始化 TIS，跳过 extractProps 检查",
                descriptorScanReachable());
        Map<String, Object> props = desc.getExtractProps(true);
        Assert.assertNotNull("extractProps 必须带 viewStyle（前端据此选列表/表格形态）",
                props.get("viewStyle"));
        Assert.assertNotNull("extractProps 必须带 colsCfg", props.get("colsCfg"));
        Assert.assertNotNull("extractProps 必须带 maxHeight", props.get("maxHeight"));
        Assert.assertFalse("shortComment 不得为空（列表里只显示短注释，空着等于没说明）",
                ((IPluginStore.MultiDescribleElementDescriptor<?>) desc).shortComment().isEmpty());
    }

    /**
     * 基类 {@code UserInputWidgetType} 零 {@code @FormField}，因此<b>不得</b>有 json ——
     * 它同时是「基类刻意不放字段」这条设计的钉子：字段一旦下沉到基类，各子类的 ordinal
     * 就会撞区间、表单顺序开始飘，而且所有子类会共享一份本不该有的 label。
     */
    @Test
    public void abstractBaseHasNoFormResource() {
        Assert.assertTrue("基类应零 @FormField 字段（字段全在各子类，各自 ordinal 从 0 起算）",
                formFieldsOf(UserInputWidgetType.class).isEmpty());
        Assert.assertNull("零字段的基类不应有 .json",
                TisPluginFormTestSupport.formResource(UserInputWidgetType.class.getName()));
    }

    // ===================================================================
    // 4. 默认项接线（跨语言）
    // ===================================================================

    /**
     * {@code PrimitiveDataType.json} 里 {@code widgetType.dftVal} 必须逐字等于
     * {@code InputTextWidget} 的 {@code displayName}。
     *
     * <p>没有任何编译期保护：前端拿 {@code dftVal} 逐项比对子类 {@code displayName}
     * 来选默认项，值写错的表现是「新增参数时默认控件悄悄拐成了别的」。
     * 断言取 {@code descriptorOf(...).getDisplayName()} 而不是写死字面量，
     * 这样改了 displayName 而忘了改 json 同样会被抓住。
     */
    @Test
    public void defaultWidgetTypeMatchesInputTextDisplayName() {
        JSONObject resource = TisPluginFormTestSupport.formResource(PrimitiveDataType.class.getName());
        Assert.assertNotNull("PrimitiveDataType.json 应存在", resource);

        JSONObject meta = resource.getJSONObject(FIELD_WIDGET_TYPE);
        Assert.assertNotNull(FIELD_WIDGET_TYPE + " 在 PrimitiveDataType.json 里应有条目", meta);
        Assert.assertEquals(FIELD_WIDGET_TYPE + " 的 dftVal 必须逐字等于 InputTextWidget 的 displayName"
                        + "（前端按它逐项比对 displayName 选默认项）",
                TisPluginFormTestSupport.descriptorOf(InputTextWidget.class).getDisplayName(),
                meta.getString("dftVal"));
    }

    // ===================================================================
    // 5. 扁平字段不得复辟
    // ===================================================================

    /**
     * {@code widgetType} 必须是抽象基类类型，不能退回 {@link FormFieldType} 枚举。
     *
     * <p>退回枚举即意味着改造被回退：枚举承载不了控件自己的配置（占位提示、上下界、候选选项……），
     * 那些字段就只好平铺回宿主类，重新变成「只在某个枚举值下有语义、靠 validateAll() 里 if 兜底」
     * 的形态。
     */
    @Test
    public void widgetTypeMustBePolymorphicNotEnum() {
        Field field;
        try {
            field = PrimitiveDataType.class.getField(FIELD_WIDGET_TYPE);
        } catch (NoSuchFieldException e) {
            throw new AssertionError("PrimitiveDataType 缺少 " + FIELD_WIDGET_TYPE + " 字段");
        }
        Assert.assertEquals("PrimitiveDataType." + FIELD_WIDGET_TYPE + " 的类型应是抽象基类",
                UserInputWidgetType.class, field.getType());
        Assert.assertNotEquals("不得退回裸枚举（枚举承载不了各控件自己的配置）",
                FormFieldType.class, field.getType());

        // 聚合属性字段不得带 type=（create-plugin skill：写了会运行时出错）。
        // 但这一条<b>无法</b>用反射证明：@FormField.type() 的默认值就是 INPUTTEXT，
        // 省略与显式写 INPUTTEXT 读出来完全一样。所以这里只钉住真正可判定的部分 ——
        // 字段类型是抽象基类（上面两条），以及 required 校验在位。
        FormField annotation = field.getAnnotation(FormField.class);
        Assert.assertNotNull("widgetType 应是聚合属性字段（带 @FormField 以便声明 ordinal/required）",
                annotation);
        Assert.assertTrue("widgetType 应带 Validator.require（控件类型是必填项）",
                Arrays.asList(annotation.validate()).contains(Validator.require));
    }

    // ===================================================================
    // 6. 下发给前端的派发键（需要 TIS 运行期）
    // ===================================================================

    /**
     * 每个子类 descriptor 的 {@code getExtractProps(true)} 都必须带 {@code formFieldType}，
     * 且 19 个值合起来恰好是 {@link FormFieldType} 全集。
     *
     * <p>这是唯一能证明「子类 ↔ 枚举」在<b>下发给前端的线格式</b>上也没写错的手段 ——
     * 前端渲染器靠这个短键派发控件。它必须放 {@code String} 而不是枚举本身：
     * extractProps 会被原样交给 fastjson，枚举序列化出来的形态前端用不了。
     *
     * <p>需要 TIS 运行期（{@code getExtractProps} 会走属性类型装载）。环境不具备时整条跳过，
     * 用 {@code -DargLine="--add-opens java.base/java.lang=ALL-UNNAMED"} 可本地跑起来。
     */
    @Test
    public void extractPropsCarryFormFieldType() {
        org.junit.Assume.assumeTrue("当前环境未初始化 TIS（JDK17 下通常还缺 "
                        + "--add-opens java.base/java.lang=ALL-UNNAMED），跳过 extractProps 检查",
                descriptorScanReachable());

        Map<String, String> dispatched = new LinkedHashMap<>();
        for (Class<?> sub : subclasses()) {
            Descriptor<?> desc = TisPluginFormTestSupport.descriptorOf(sub);
            Map<String, Object> props = desc.getExtractProps(true);
            Object token = props.get(KEY_FORM_FIELD_TYPE);
            Assert.assertNotNull(sub.getSimpleName() + " 的 extractProps 缺少 " + KEY_FORM_FIELD_TYPE
                    + "（前端拿不到派发键就渲染不出对应控件）", token);
            Assert.assertTrue(KEY_FORM_FIELD_TYPE + " 必须是 String（枚举交给 fastjson 会序列化成前端用不了的形态）",
                    token instanceof String);
            dispatched.put((String) token, sub.getSimpleName());
        }

        for (FormFieldType t : FormFieldType.values()) {
            Assert.assertTrue("FormFieldType." + t + " 没有任何子类下发出该派发键",
                    dispatched.containsKey(t.name()));
        }
        Assert.assertEquals("下发的派发键数应与 FormFieldType 常量数相等", FormFieldType.values().length,
                dispatched.size());
    }

    // ===================================================================
    //  helpers
    // ===================================================================

    /** sezpoz 索引里 {@link UserInputWidgetType} 的具体子类（不含基类自身与抽象类） */
    private static Set<Class<?>> subclasses() {
        return TisPluginFormTestSupport.concreteSubclassesOf(UserInputWidgetType.class, IMPL_PKG);
    }

    private static UserInputWidgetType instantiate(Class<?> clazz) {
        try {
            return (UserInputWidgetType) clazz.getDeclaredConstructor().newInstance();
        } catch (Throwable t) {
            throw new AssertionError(clazz.getSimpleName() + " 无法反射实例化"
                    + "（子类应保持无参构造、字段初始化不得抛异常）", t);
        }
    }

    /** 声明的表单字段：{@code @FormField} 与 {@code @SubForm} 都算（两者都对应 json 里的一个条目） */
    private static List<Field> formFieldsOf(Class<?> clazz) {
        List<Field> result = new ArrayList<>();
        for (Field f : clazz.getDeclaredFields()) {
            if (f.getAnnotation(FormField.class) != null || f.getAnnotation(SubForm.class) != null) {
                result.add(f);
            }
        }
        return result;
    }

    /**
     * TIS 的 descriptor 扫描是否可用。
     *
     * <p>{@code DescriptorExtensionList} 是惰性的：真正的扩展扫描发生在迭代上，
     * 所以必须在这个 try 里就把它物化，否则异常会漏到外面变成失败而非跳过。
     */
    private static boolean descriptorScanReachable() {
        try {
            TIS tis = TIS.get();
            return tis != null && !tis.getDescriptorList(UserInputWidgetType.class).isEmpty();
        } catch (Throwable t) {
            return false;
        }
    }

    /** 枚举按名字排序的小工具（{@code FormFieldType} 未实现 {@code Comparable}） */
    private static final class ComparatorHolder {
        static final java.util.Comparator<FormFieldType> BY_NAME =
                java.util.Comparator.comparing(Enum::name);
    }
}
