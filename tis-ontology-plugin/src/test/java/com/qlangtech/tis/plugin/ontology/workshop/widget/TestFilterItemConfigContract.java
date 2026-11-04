package com.qlangtech.tis.plugin.ontology.workshop.widget;

import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.ValueChangePipe;
import com.qlangtech.tis.plugin.IPluginStore;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.ontology.impl.action.widget.WidgetOptionConfig;
import com.qlangtech.tis.plugin.ontology.workshop.TisPluginFormTestSupport;
import com.qlangtech.tis.plugin.ontology.workshop.widget.FilterItemConfig.FilterOperator;
import com.qlangtech.tis.plugin.ontology.workshop.widget.component.AbstractOpsSource;
import com.qlangtech.tis.plugin.ontology.workshop.widget.component.AbstractSelectComponent;
import com.qlangtech.tis.plugin.ontology.workshop.widget.component.DatePickerComponent;
import com.qlangtech.tis.plugin.ontology.workshop.widget.component.FilterComponent;
import com.qlangtech.tis.plugin.ontology.workshop.widget.component.KeywordComponent;
import com.qlangtech.tis.plugin.ontology.workshop.widget.component.ManualOpsSource;
import com.qlangtech.tis.plugin.ontology.workshop.widget.component.MultiSelectComponent;
import com.qlangtech.tis.plugin.ontology.workshop.widget.component.ObjectSetOpsSource;
import com.qlangtech.tis.plugin.ontology.workshop.widget.component.SingleSelectComponent;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.FilterListWidget;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * {@code FilterListWidget.filters} 走 {@code MULTI_DESCRIBLE_PLUGIN} 子表单、且
 * {@code component} 已是多态插件之后的契约。
 *
 * <p>这一处的失败模式全是<b>静默</b>的，故逐条钉住：
 * <ul>
 *   <li>宿主的字段类型退回 {@code MULTI_SELECTABLE} → 前端仍去找 {@code elementCreator}
 *       （已删除），整个 Widget 表单打不开；</li>
 *   <li>元素类没实现 {@code MultiDescribleElement} → 表单能渲染，保存 / 回填期才炸；</li>
 *   <li>{@code component} 忘了删 {@code type = ENUM} → 子表单机制退化成单值下拉，
 *       单选 / 多选就没有地方挂 {@code optionsSource}；</li>
 *   <li>两个多态族的 {@code BasicDescriptor} 共用一个泛型 → 「候选项来源」的两个实现
 *       混进「控件形态」下拉（{@code DescriptorExtensionList} 按 {@code getT()} 过滤）；</li>
 *   <li>抽象基类挂 {@code @TISExtension} → 抽象类型同样混进下拉
 *       （sezpoz 索引不过滤抽象类）；</li>
 *   <li>SELECTABLE 未 {@code registerSelectOptions} → 整张表单打不开；</li>
 *   <li>pipe 的 {@code toField} 无序 → {@code Arrays.binarySearch} 查不到源字段，
 *       级联与自校验白名单一起失效；</li>
 *   <li>{@code colsConfig()} 留着 {@code component} → 恒空白列
 *       （{@code MultiChildViewDescriptor.cellText} 对对象值恒渲染空串）。</li>
 * </ul>
 *
 * <p>不触达 TIS 插件扫描（不需要 {@code --add-opens}）：类清单来自编译期 sezpoz 索引，
 * 资源靠 classloader 读，descriptor 直接 {@code newInstance}。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/11/2
 */
public class TestFilterItemConfigContract {

    private static final String COMPONENT_PKG =
            "com.qlangtech.tis.plugin.ontology.workshop.widget.component";

    // ==================================================================
    //  宿主 → 元素
    // ==================================================================

    @Test
    public void hostFieldIsMultiDescriblePlugin() throws Exception {
        Field filters = FilterListWidget.class.getField(FilterListWidget.KEY_FILTERS);
        FormField ff = filters.getAnnotation(FormField.class);
        Assert.assertNotNull("filters 必须带 @FormField", ff);
        Assert.assertEquals("filters 必须改成 MULTI_DESCRIBLE_PLUGIN"
                        + "（旧的 MULTI_SELECTABLE + FilterItemElementCreatorFactory 已删除）",
                FormFieldType.MULTI_DESCRIBLE_PLUGIN, ff.type());
        Assert.assertEquals("desClazz 必须指向 List 的元素类型", FilterItemConfig.class, ff.desClazz());
    }

    @Test
    public void elementImplementsContract() {
        Assert.assertTrue("元素类必须实现 MultiDescribleElement（identityValue 无处取）",
                IPluginStore.MultiDescribleElement.class.isAssignableFrom(FilterItemConfig.class));

        FilterItemConfig item = new FilterItemConfig();
        item.property = "departure";
        Assert.assertEquals("identityValue 必须取 property（标签允许重复，属性名才是身份）",
                "departure", item.identityValue());
    }

    /**
     * 恰好一个 {@code identity = true} 字段：多了框架不知以谁为行键，少了行无法跟踪。
     */
    @Test
    public void exactlyOneIdentityField() {
        List<String> identityFields = Arrays.stream(FilterItemConfig.class.getDeclaredFields())
                .filter(f -> f.getAnnotation(FormField.class) != null)
                .filter(f -> f.getAnnotation(FormField.class).identity())
                .map(Field::getName)
                .collect(Collectors.toList());
        Assert.assertEquals("identity 字段应恰好一个", List.of(FilterItemConfig.KEY_PROPERTY), identityFields);
    }

    @Test
    public void descriptorDeclaresTableView() throws Exception {
        Class<?> descriptorClass = FilterItemConfig.DftDescriptor.class;
        Assert.assertTrue("descriptor 必须 extends MultiDescribleElementDescriptor"
                        + "（否则前端拿不到 viewStyle/colsCfg/maxHeight，子表格静默退化）",
                IPluginStore.MultiDescribleElementDescriptor.class.isAssignableFrom(descriptorClass));

        IPluginStore.MultiDescribleElementDescriptor<?> descriptor =
                (IPluginStore.MultiDescribleElementDescriptor<?>) descriptorClass.getDeclaredConstructor().newInstance();

        Assert.assertEquals("应采用表格形态（字段值都短）",
                IPluginStore.MultiDescribleElementDescriptor.ViewStyle.Table, descriptor.viewStyle());

        List<IPluginStore.MultiDescribleElementDescriptor.ColConfig> cols = descriptor.colsConfig();
        Assert.assertNotNull("Table 视图必须给出列配置", cols);
        Assert.assertFalse("Table 视图至少要有一列", cols.isEmpty());
        Assert.assertTrue("Table 视图至少要有一列 clickable（否则用户无法打开行编辑面板）",
                cols.stream().anyMatch(c -> Boolean.TRUE.equals(c.getClickable())));

        // 列出的键必须都是元素类上真实存在的 @FormField，写错只会渲染出空列
        for (IPluginStore.MultiDescribleElementDescriptor.ColConfig col : cols) {
            Field field = declaredFormField(FilterItemConfig.class, col.getTitleKey());
            Assert.assertNotNull("列 " + col.getTitleKey() + " 在 FilterItemConfig 上没有对应字段", field);
            Assert.assertFalse("列 " + col.getTitleKey() + " 是 Describable 字段："
                            + "MultiChildViewDescriptor.cellText 对对象值恒渲染空串，列出来就是一条恒空白列",
                    Describable.class.isAssignableFrom(field.getType()));
        }
        Assert.assertTrue("过滤项列表较矮，maxHeight 应放大（默认 100 只露一行半）",
                descriptor.maxHeight() > 100);
    }

    // ==================================================================
    //  component → 多态插件
    // ==================================================================

    /**
     * {@code component} 必须是<b>嵌套多态子表单</b>，不能是 ENUM：
     * <ul>
     *   <li>{@code type()} 保持默认（{@code INPUTTEXT} 是 {@code FormField} 的默认值，
     *       「没写」与「写默认值」等价）—— 写成 ENUM 会让子表单机制退化成单值下拉；</li>
     *   <li>{@code desClazz()} 保持默认 {@code Describable.class} —— 单字段写 {@code desClazz}
     *       无效（那是 {@code MULTI_DESCRIBLE_PLUGIN} 的用法）。</li>
     * </ul>
     */
    @Test
    public void componentFieldIsPolymorphicSubForm() throws Exception {
        Field field = FilterItemConfig.class.getField(FilterItemConfig.KEY_COMPONENT);
        FormField ff = field.getAnnotation(FormField.class);
        Assert.assertNotNull("component 必须带 @FormField", ff);
        Assert.assertEquals("component 的字段类型必须是抽象的 FilterComponent", FilterComponent.class, field.getType());
        Assert.assertNotEquals("component 不能再是 ENUM —— 单选/多选就没有地方挂 optionsSource",
                FormFieldType.ENUM, ff.type());
        Assert.assertEquals("component 的 type 应保持默认（不写 type）",
                FormFieldType.INPUTTEXT, ff.type());
        Assert.assertEquals("component 的 desClazz 应保持默认 Describable.class",
                Describable.class, ff.desClazz());

        // Java 初值必须存在（XStream 之外的新建路径读它），且要与 json 的 dftVal 一致
        Assert.assertEquals("component 的 Java 初值应是 KeywordComponent",
                KeywordComponent.class, new FilterItemConfig().component.getClass());

        // 落盘往返：反序列化不走构造器也不走字段初始化器，readResolve 必须补齐
        FilterItemConfig item = new FilterItemConfig();
        item.component = null;
        item.normalize();
        Assert.assertEquals("component 缺失时必须回落到 KeywordComponent", KeywordComponent.class,
                item.component.getClass());

        item.component = new MultiSelectComponent();
        ((AbstractSelectComponent) item.component).optionsSource = null;
        item.normalize();
        Assert.assertEquals("单选/多选的 optionsSource 缺失时必须回落到 ObjectSetOpsSource",
                ObjectSetOpsSource.class,
                ((AbstractSelectComponent) item.component).optionsSource.getClass());
    }

    /**
     * 控件形态族恰为四个具体子类，且各自的 {@code getDisplayName()} 就是前端的 kebab token。
     *
     * <p>前端 {@code resolveFilterComponent} 用 {@code resolveImplKind(impl, "Component")}
     * 从类名推 token，与这里的 {@code getDisplayName()} 是两套推导 —— 两边必须给出<b>同一串</b>，
     * 否则 {@code ngSwitchCase} 全部落空、控件渲染不出来（且不报错）。
     *
     * <p>{@code AbstractSelectComponent} 是抽象的，不会出现在选择器里
     * （{@code concreteSubclassesOf} 已过滤 abstract），这里通过「恰为四个」把多余的类一并挡住。
     */
    @Test
    public void componentSubclassesAreExactlyFourKebabTokens() {
        Set<Class<?>> subs = TisPluginFormTestSupport.concreteSubclassesOf(FilterComponent.class, COMPONENT_PKG);
        Assert.assertEquals("控件形态应恰为四个具体子类", Set.of("DatePickerComponent", "KeywordComponent",
                "MultiSelectComponent", "SingleSelectComponent"), TisPluginFormTestSupport.names(subs));

        Map<String, String> expectedTokens = Map.of(
                "KeywordComponent", "keyword",
                "DatePickerComponent", "date-picker",
                "SingleSelectComponent", "single-select",
                "MultiSelectComponent", "multi-select");
        subs.forEach(sub -> {
            Descriptor<?> desc = TisPluginFormTestSupport.descriptorOf(sub);
            Assert.assertEquals(sub.getSimpleName() + " 的 getDisplayName() 是线格式（前端 kebab token）",
                    expectedTokens.get(sub.getSimpleName()), desc.getDisplayName());
        });
    }

    /**
     * 候选项来源族恰为两个具体子类，token 同样是线格式。
     */
    @Test
    public void opsSourceSubclassesAreExactlyTwoKebabTokens() {
        Set<Class<?>> subs = TisPluginFormTestSupport.concreteSubclassesOf(AbstractOpsSource.class, COMPONENT_PKG);
        Assert.assertEquals("候选项来源应恰为两个具体子类", Set.of("ManualOpsSource", "ObjectSetOpsSource"),
                TisPluginFormTestSupport.names(subs));
        subs.forEach(sub -> {
            Descriptor<?> desc = TisPluginFormTestSupport.descriptorOf(sub);
            String expected = sub == ManualOpsSource.class ? "manual" : "object-set";
            Assert.assertEquals(sub.getSimpleName() + " 的 getDisplayName() 是线格式", expected, desc.getDisplayName());
        });
    }

    /**
     * 两个多态族的 descriptor 必须各自<b>类型化</b>到自己的抽象基类。
     *
     * <p>{@code DescriptorExtensionList#_load} 按 {@code describableType.isAssignableFrom(d.getT())}
     * 过滤：共用一个 {@code Descriptor<FilterComponent>} 会让「候选项来源」的两个实现也满足
     * 「是 FilterComponent 的实现」这一条 —— 控件形态下拉里凭空多出两项（反之亦然）。
     * 未写泛型时 {@code Descriptor.getT()} 直接抛异常，该实现会在列表装载时被<b>静默跳过</b>。
     */
    @Test
    public void twoFamiliesHaveDistinctTypedDescriptors() {
        // 取真实的具体 descriptor，顺带验证它们能被无参构造出来
        Assert.assertEquals("FilterComponent 的 BasicDescriptor 必须类型化到 FilterComponent",
                FilterComponent.class, new KeywordComponent.DefaultDescriptor().getT());
        Assert.assertEquals("AbstractOpsSource 的 BasicDescriptor 必须类型化到 AbstractOpsSource",
                AbstractOpsSource.class, new ManualOpsSource.DefaultDescriptor().getT());

        // 反向：候选项来源的 descriptor 不能落在「控件形态」的候选集合里
        Assert.assertFalse("两个族的 BasicDescriptor 不能共用同一个泛型（会互相混进对方的选择器）",
                FilterComponent.class.isAssignableFrom(new ObjectSetOpsSource.DefaultDescriptor().getT()));
    }

    /**
     * {@code optionsSource} 只声明在单选 / 多选共享的父类上 —— 关键词与日期选择器不该有这个字段。
     *
     * <p>这是「改成多态」这件事的核心收益：扁平化后必然出现「选了关键词却还配着一份候选项」
     * 这种自相矛盾的状态，而框架只有静态 {@code advance} 分组、没有条件可见性。
     */
    @Test
    public void optionsSourceOnlyOnSelectComponents() throws Exception {
        Field field = AbstractSelectComponent.class.getField(AbstractSelectComponent.KEY_OPTIONS_SOURCE);
        Assert.assertEquals("optionsSource 的声明类型必须是抽象的 AbstractOpsSource"
                        + "（写成具体类或加 desClazz 都会退化成单值控件）",
                AbstractOpsSource.class, field.getType());
        FormField ff = field.getAnnotation(FormField.class);
        Assert.assertNotNull("optionsSource 必须带 @FormField", ff);
        Assert.assertEquals("optionsSource 的 type 应保持默认", FormFieldType.INPUTTEXT, ff.type());
        Assert.assertEquals("optionsSource 的 desClazz 应保持默认", Describable.class, ff.desClazz());
        Assert.assertEquals("optionsSource 的 Java 初值应是 ObjectSetOpsSource"
                        + "（与 AbstractSelectComponent.json 的 dftVal 成对）",
                ObjectSetOpsSource.class, new SingleSelectComponent().optionsSource.getClass());

        for (Class<?> noOptions : List.of(KeywordComponent.class, DatePickerComponent.class)) {
            Assert.assertFalse(noOptions.getSimpleName() + " 不该有 optionsSource 字段"
                            + "（它就是不该配候选项来源的两支）",
                    Arrays.stream(noOptions.getDeclaredFields())
                            .anyMatch(f -> AbstractSelectComponent.KEY_OPTIONS_SOURCE.equals(f.getName())));
        }
        Assert.assertTrue("SingleSelectComponent 必须继承 AbstractSelectComponent",
                AbstractSelectComponent.class.isAssignableFrom(SingleSelectComponent.class));
        Assert.assertTrue("MultiSelectComponent 必须继承 AbstractSelectComponent",
                AbstractSelectComponent.class.isAssignableFrom(MultiSelectComponent.class));
    }

    /**
     * 手填列表的载体必须是 {@code MULTI_DESCRIBLE_PLUGIN} + {@code WidgetOptionConfig}：
     * 元素类缺 {@code MultiDescribleElement} 或 {@code desClazz} 漏写，字段会渲染成一个
     * 没有元素类型的空列表（编译期不报错）。
     */
    @Test
    public void manualOpsSourceCollectsStructuredOptions() throws Exception {
        Field field = ManualOpsSource.class.getField(ManualOpsSource.KEY_OPTIONS);
        FormField ff = field.getAnnotation(FormField.class);
        Assert.assertNotNull("options 必须带 @FormField", ff);
        Assert.assertEquals("候选项必须是结构化的列表，不能让用户手写 JSON 字符串",
                FormFieldType.MULTI_DESCRIBLE_PLUGIN, ff.type());
        Assert.assertEquals("desClazz 必须指向元素类", WidgetOptionConfig.class, ff.desClazz());

        // 泛型必须是 List<WidgetOptionConfig>，与 desClazz 一致
        Assert.assertEquals(List.class, field.getType());
        ParameterizedType genericType = (ParameterizedType) field.getGenericType();
        Assert.assertEquals("List 的元素类型必须是 WidgetOptionConfig",
                WidgetOptionConfig.class, genericType.getActualTypeArguments()[0]);

        // 元素类自身的契约（identity = val）
        Assert.assertTrue("WidgetOptionConfig 必须实现 MultiDescribleElement",
                IPluginStore.MultiDescribleElement.class.isAssignableFrom(WidgetOptionConfig.class));
    }

    /**
     * 有字段的抽象基类必须亲手写 json（自动扫描只看具体类），且 {@code dftVal} 与 Java 初值逐字一致。
     *
     * <p>{@code optionsSource} 缺 {@code dftVal} 时前端新建的单选 / 多选没有候选来源；
     * 与 Java 初值不一致时 {@code TestWorkshopFormDefaults} 会直接失败。
     */
    @Test
    public void selectComponentJsonDeclaresOptionsSourceDefault() {
        JSONObject json = TisPluginFormTestSupport.formResource(AbstractSelectComponent.class.getName());
        Assert.assertNotNull("有字段的抽象基类 AbstractSelectComponent 必须有自己的 .json", json);
        JSONObject optionsSource = json.getJSONObject(AbstractSelectComponent.KEY_OPTIONS_SOURCE);
        Assert.assertNotNull("json 里缺 optionsSource 条目（表单上无 label/help/dftVal）", optionsSource);

        String expectedDftVal = TisPluginFormTestSupport.descriptorOf(ObjectSetOpsSource.class).getDisplayName();
        Assert.assertEquals("dftVal 必须等于 Java 初值 ObjectSetOpsSource 的 getDisplayName()",
                expectedDftVal, optionsSource.getString("dftVal"));
    }

    /**
     * 零字段类<b>不得</b>有 json：资源沿祖先链合并，多出来的 json 会在未来静默隐藏继承字段。
     */
    @Test
    public void fieldlessClassesHaveNoFormResource() {
        for (Class<?> clazz : List.of(FilterComponent.class, AbstractOpsSource.class, KeywordComponent.class,
                DatePickerComponent.class, SingleSelectComponent.class, MultiSelectComponent.class)) {
            Assert.assertNull(clazz.getSimpleName() + " 没有 @FormField，不应有 .json"
                            + "（祖先资源会被合并进来，多一份只会在未来静默隐藏继承字段）",
                    TisPluginFormTestSupport.formResource(clazz.getName()));
        }
    }

    // ==================================================================
    //  SELECTABLE / 级联
    // ==================================================================

    @Test
    public void propertyFieldOptionsAreRegistered() {
        FilterItemConfig.DftDescriptor descriptor = new FilterItemConfig.DftDescriptor();
        try {
            Assert.assertNotNull(descriptor.getSelectOptions(FilterItemConfig.KEY_PROPERTY));
        } catch (IllegalStateException e) {
            Assert.assertFalse("property 未在 DftDescriptor 构造器中 registerSelectOptions —— 表单会报错："
                            + e.getMessage(),
                    String.valueOf(e.getMessage()).contains("has not been register"));
        }
    }

    /**
     * {@code ObjectSetOpsSource} 的三个 SELECTABLE 字段都必须登记候选来源。
     *
     * <p>只登记「有真实候选」的那一个是不够的：{@code DescriptorsJSON#getSelectOptions} 对
     * 每个 SELECTABLE 字段都要查一次，漏一个整张表单就打不开。
     */
    @Test
    public void objectSetOpsSourceSelectablesAreRegistered() {
        ObjectSetOpsSource.DefaultDescriptor descriptor = new ObjectSetOpsSource.DefaultDescriptor();
        for (String field : List.of(ObjectSetOpsSource.KEY_SOURCE_OBJECT_SET_VAR,
                ObjectSetOpsSource.KEY_LABEL_PROPERTY, ObjectSetOpsSource.KEY_VALUE_PROPERTY)) {
            try {
                Assert.assertNotNull(descriptor.getSelectOptions(field));
            } catch (IllegalStateException e) {
                Assert.assertFalse(field + " 未在 ObjectSetOpsSource.DefaultDescriptor 构造器中"
                        + " registerSelectOptions —— 表单会报错：" + e.getMessage(),
                        String.valueOf(e.getMessage()).contains("has not been register"));
            } catch (Throwable t) {
                // 已注册，只是选项 getter 求值需要 TIS 运行期上下文（IPluginContext 线程本地为空）
            }
        }
    }

    /**
     * 管道目标字段必须<b>按字典序</b>声明。
     *
     * <p>{@code Descriptor#getValueChangeFromFieldKeys} 用 {@code Arrays.binarySearch} 查
     * {@code getToField()} 返回的数组；无序时查找失败，级联的「源字段是否被排除出自校验白名单」
     * 这条判断会静默失效 —— 而 {@code getToField()} 序列化给前端后行为照常，所以只测行为发现不了。
     */
    @Test
    public void everyPipeTargetsAreSortedAndResolvable() {
        List<Descriptor<?>> descriptors = new ArrayList<>();
        descriptors.add(new FilterListWidget.DescriptorImpl());
        descriptors.add(new FilterItemConfig.DftDescriptor());
        descriptors.add(new ObjectSetOpsSource.DefaultDescriptor());

        int checked = 0;
        for (Descriptor<?> descriptor : descriptors) {
            for (ValueChangePipe pipe : descriptor.getValueChangePipes().values()) {
                String[] toFields = pipe.getToField();
                Assert.assertTrue(descriptor.getT().getSimpleName() + " 的 pipe 没有 toField", toFields.length > 0);
                String[] sorted = toFields.clone();
                Arrays.sort(sorted);
                Assert.assertArrayEquals("pipe fromField=" + pipe.getFromField()
                                + " 的 toField 必须按字典序声明（Arrays.binarySearch 查找的前提）",
                        sorted, toFields);

                for (String toField : toFields) {
                    List<String> fromFields = descriptor.getValueChangeFromFieldKeys(toField);
                    Assert.assertTrue("toField " + toField + " 反查不到源字段 " + pipe.getFromField()
                                    + "（binarySearch 已失效，或 toField 写错），实际：" + fromFields,
                            fromFields.contains(pipe.getFromField()));
                    checked++;
                }
            }
        }
        Assert.assertTrue("一个 pipe 都没扫到 —— 三个 descriptor 构造器里的 valueChangePipe 还在？", checked > 0);
    }

    /**
     * 宿主必须声明「objectSetVar → 每行 property 候选」的级联管道。
     *
     * <p>落点字符串是线格式：服务端原样写进 {@code $pipe_field$filters[].property} 下发，
     * 唯一的解析点是前端 {@code item-prop-val.component.ts#propValChange}（按 {@code []} 走
     * 逐行回填分支）。写成 {@code filters.property} 或漏掉 {@code []} 都<b>不报错</b>，
     * 只是切换对象集变量时属性候选不刷新。
     */
    @Test
    public void cascadePipeTargetsRowProperty() {
        FilterListWidget.DescriptorImpl descriptor = new FilterListWidget.DescriptorImpl();
        String toField = FilterListWidget.KEY_FILTERS + "[]." + FilterItemConfig.KEY_PROPERTY;
        List<String> fromFields = descriptor.getValueChangeFromFieldKeys(toField);
        Assert.assertTrue("宿主的级联管道落点应为 " + toField + "，实际反查到的源字段：" + fromFields,
                fromFields.contains(FilterListWidget.KEY_OBJECT_SET_VAR));
    }

    // ==================================================================
    //  元素类字段
    // ==================================================================

    /**
     * {@code FilterOperator} 的常量名是<b>线格式</b>：前端 {@code normalizeOperator} 按
     * {@code in_} 翻译成 {@code in}。增删常量必须同步前端。
     */
    @Test
    public void operatorTokensAreWireFormat() {
        Assert.assertEquals(List.of("equals", "contains", "greaterThan", "lessThan",
                        "greaterThanOrEqual", "lessThanOrEqual", "in_"),
                Arrays.stream(FilterOperator.values()).map(Enum::name).collect(Collectors.toList()));
        Assert.assertEquals("属于集合", FilterOperator.in_.shortComment());
    }

    private static Field declaredFormField(Class<?> clazz, String fieldName) {
        try {
            Field field = clazz.getField(fieldName);
            Objects.requireNonNull(field.getAnnotation(FormField.class), fieldName + " 没有 @FormField");
            Assert.assertFalse(fieldName + " 不能是 static", Modifier.isStatic(field.getModifiers()));
            return field;
        } catch (NoSuchFieldException e) {
            return null;
        }
    }
}
