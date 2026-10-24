package com.qlangtech.tis.plugin.ontology.workshop.model.section;

import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.ElementPluginDesc;
import com.qlangtech.tis.extension.IPropertyType;
import com.qlangtech.tis.extension.impl.PropertyType;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.ontology.workshop.TisPluginFormTestSupport;
import com.qlangtech.tis.plugin.ontology.workshop.model.AbstractWorkshopSection;
import com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopSection;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * {@code WorkshopSection} 的「布局」多态字段改造接线回归测试。
 *
 * <p>本轮把一对强相关字段 —— {@code layout}（枚举 {@code enums.SectionLayout}，
 * 值域 COLUMNS/ROWS/TABS/FLOW/TOOLBAR/LOOP）与 {@code layoutConfig}（抽象类
 * {@code model.section.SectionLayoutConfig}，子类只有 Columns/Loop/Tabs 三个）——
 * <b>合并</b>成单个多态字段 {@code layout}，类型为新的抽象基类
 * {@link SectionLayout}（即原 {@code SectionLayoutConfig} 改名而来），六个子类与
 * 原枚举常量 1:1 对应。
 *
 * <p>改造前那对字段是 TIS 已判定为反模式的形态：<b>Describable 多态体系旁挂平行枚举</b>。
 * 它允许 {@code layout = COLUMNS} 配 {@code layoutConfig = TabsLayoutConfig} 这种
 * 自相矛盾的状态能编译、能过校验、能落盘；而且两套分类体系已经漂移 —— 枚举 6 个常量
 * 对 3 个子类。合并后「是哪种布局」完全由 <b>Java 类型</b>承担，矛盾态不再可表示。
 *
 * <p>与 {@code model.header.TestWorkshopHeaderConfig} 同构。这类改造有一批<b>没有任何
 * 编译期保护</b>的接线，全部由本类钉住：
 *
 * <ol>
 *   <li><b>子类形态</b> —— 具体子类集合与期望的六个双向相等（用双向相等而非
 *       「扫到的 ⊆ 期望」：后者在扫到 0 个时循环体不执行，会假通过）</li>
 *   <li><b>descriptor 的 id</b> —— 每个子类的 {@code getId()} 必须等于它<b>自己</b>的 FQCN。
 *       descriptor 一旦被提到基类上，六个子类 id 撞号，前端再也分辨不出 impl</li>
 *   <li><b>短 key 推导（跨语言，最容易静默错位的一条）</b> —— 前端的
 *       {@code resolveImplKind(impl, 'Layout')} 从 FQCN 推出短 key，必须逐字等于
 *       {@code SECTION_LAYOUT_OPTIONS} 的取值 {@code columns/rows/tabs/flow/toolbar/loop}。
 *       本类<b>把那个 TS 函数按同样的两条正则抄了一份</b>在 {@link #resolveImplKind}
 *       里，两边同规则才能真的互相印证；只断言「类名以 Layout 结尾」是抓不住错位的</li>
 *   <li><b>json 的存在性规则</b> —— 带 {@code @FormField} 的子类<b>必须</b>有自己的
 *       {@code .json}；零字段的子类<b>必须没有</b></li>
 *   <li><b>扁平字段不得复辟</b> —— {@code layoutConfig} 不允许再出现在 {@code WorkshopSection}
 *       上；{@code layout} 的类型必须是抽象基类，且 {@code WorkshopSection.json} 的
 *       {@code layout} 块不得再带 {@code enum} 数组（那是它曾是 {@code FormFieldType.ENUM}
 *       的痕迹，留着会让前端按枚举渲染而不出子表单）</li>
 * </ol>
 *
 * <p>全部断言<b>不依赖 TIS 运行期</b>（不需要 {@code --add-opens}）：类清单来自编译期生成的
 * sezpoz 索引，资源靠 classloader 读，descriptor 直接 {@code newInstance} —— 与
 * {@code TestWorkshopHeaderConfig} 的取舍一致，避免整类因为环境问题变成「一直绿但什么都没查」。
 */
public class TestWorkshopSectionConfig {

    /** 本轮改造所在的包 */
    private static final String SECTION_PKG = "com.qlangtech.tis.plugin.ontology.workshop.model.section.";

    /**
     * 六个子类 —— 与改造前枚举的六个常量 1:1。
     *
     * <p>刻意保持 1:1（其中 Rows/Flow/Toolbar 三个无字段）而不是只留「有参数的」三个：
     * 若只留三个，另外三个布局就没有载体，只能退回某种回退分支（比如「impl 不认得时
     * 当 rows」），那种分支正是本次改造要消灭的东西。
     */
    private static final List<Class<?>> LAYOUT_SUBCLASSES = Arrays.asList(
            ColumnsLayout.class, RowsLayout.class, TabsLayout.class,
            FlowLayout.class, ToolbarLayout.class, LoopLayout.class);

    /** 期望的短 key —— 与前端 {@code SectionLayout} 联合类型的六个字面量逐字相同 */
    private static final Set<String> EXPECTED_KINDS = new TreeSet<>(Arrays.asList(
            "columns", "rows", "tabs", "flow", "toolbar", "loop"));

    /**
     * 零字段的子类：它们只有布局语义、没有参数，因此<b>不应</b>有 {@code .json}
     * （有 json 就意味着有人往「无参数」的布局里加了字段）。
     */
    private static final List<Class<?>> FIELDLESS_SUBCLASSES =
            Arrays.asList(RowsLayout.class, ToolbarLayout.class);

    // ===================================================================
    // 1. 子类形态
    // ===================================================================

    /** {@code SectionLayout} 恰好六个具体子类，与改造前枚举的六个常量 1:1。 */
    @Test
    public void sectionLayoutHasExactlySixSubclasses() {
        Set<Class<?>> actual = TisPluginFormTestSupport.concreteSubclassesOf(SectionLayout.class, SECTION_PKG);
        Assert.assertEquals("SectionLayout 应恰好六个具体子类（对应原枚举的六个常量）",
                simpleNames(LAYOUT_SUBCLASSES), TisPluginFormTestSupport.names(actual));
    }

    /**
     * 六个子类都不是关态，且都实现了 {@link DescriptorUseableShortComment}。
     *
     * <p>没有关态是刻意的：布局是必选维度，不存在「不设置布局」—— 它就是
     * {@code WorkshopSection} 上标了 {@code Validator.require} 的字段。
     *
     * <p>{@code shortComment()} 是 impl 下拉里显示的那行中文。没实现接口时不报错，
     * 下拉项只剩一行 FQCN，用户没法选。
     */
    @Test
    public void subclassesAreNonOffAndProvideShortComment() {
        for (Class<?> sub : LAYOUT_SUBCLASSES) {
            Assert.assertTrue(sub.getSimpleName() + " 应实现 DescriptorUseableShortComment"
                            + "（否则 impl 下拉里只剩一行 FQCN，且不报错）",
                    DescriptorUseableShortComment.class.isAssignableFrom(
                            TisPluginFormTestSupport.descriptorOf(sub).getClass()));

            Assert.assertNotEquals(sub.getSimpleName() + " 是布局子类，其 displayName 不得是 SWITCH_OFF"
                            + "（布局没有「不设置」这个状态）",
                    Descriptor.SWITCH_OFF, TisPluginFormTestSupport.descriptorOf(sub).getDisplayName());

            String shortComment = ((DescriptorUseableShortComment)
                    TisPluginFormTestSupport.descriptorOf(sub)).shortComment();
            Assert.assertNotNull(sub.getSimpleName() + ".shortComment() 不得为 null", shortComment);
            Assert.assertFalse(sub.getSimpleName() + ".shortComment() 不得为空串", shortComment.isEmpty());
        }
    }

    /**
     * 六个子类 descriptor 的 {@code getId()} 都必须等于<b>它自己</b>的 FQCN。
     *
     * <p>{@code Descriptor()} 无参构造把 {@code clazz} 置为 {@code getClass().getEnclosingClass()}，
     * 所以只要 descriptor 嵌在具体子类里就自然成立；一旦有人图省事把 descriptor 提到
     * {@link SectionLayout.BasicDescriptor} 上（或写 {@code super(SectionLayout.class)}），
     * 六个子类 {@code getId()} 全撞成同一个，前端在 impl 下拉里再也分不出选了哪一个。
     *
     * <p>{@code getId()} 同时也是前端映射表 {@code SECTION_LAYOUT_IMPL} 里那六个字面量的来源，
     * 所以这条断言等于在钉住那张表的正确性。
     */
    @Test
    public void descriptorsIdentifyTheirConcreteSubclass() {
        for (Class<?> sub : LAYOUT_SUBCLASSES) {
            Assert.assertEquals(sub.getSimpleName() + " 的 descriptor.getId() 应等于其自身 FQCN"
                            + "（同族子类撞成同一个 id 时前端无法区分 impl）",
                    sub.getName(), TisPluginFormTestSupport.descriptorOf(sub).getId());
        }
    }

    // ===================================================================
    // 2. 短 key 推导（跨语言约定，最容易静默错位）
    // ===================================================================

    /**
     * 每个子类的 FQCN 经 {@code resolveImplKind(impl, 'Layout')} 推导出的短 key，
     * 必须逐字落在前端 {@code SectionLayout} 联合类型的六个字面量里。
     *
     * <p>这条是本族的核心约定：<b>子类名必须以 {@code Layout} 结尾</b>，前端才能靠剥这个
     * 后缀直接得到短 key，从而不需要任何映射表。推导规则抄自
     * {@code tis-console/src/workshop/models/click-action.model.ts} 的
     * {@code resolveImplKind}（两条大小写边界正则 + 转小写），两边同规则才有印证价值。
     *
     * <p>漏一个大小写边界就会静默错位：{@code ColumnsLayout} 推出 {@code columns} 是对的，
     * 但若有人把类名写成 {@code Columnslayout} 或加个前缀 {@code ColumnsSectionLayout}，
     * 推导结果就落不进联合类型 —— 而 {@code resolveSectionLayoutKind} 会返回
     * {@code undefined}，配置面板<b>拒绝写入</b>，表现为「改了布局没反应」，一行日志都没有。
     */
    @Test
    public void fqcnDerivesExpectedShortKind() {
        Set<String> derived = new TreeSet<>();
        Map<String, String> detail = new LinkedHashMap<>();
        for (Class<?> sub : LAYOUT_SUBCLASSES) {
            String impl = TisPluginFormTestSupport.descriptorOf(sub).getId();
            String kind = resolveImplKind(impl, "Layout");
            detail.put(sub.getSimpleName(), impl + " -> " + kind);
            derived.add(kind);
        }
        Assert.assertEquals("六个子类 FQCN 推导出的短 key 应逐字等于前端 SectionLayout 联合类型的取值"
                + "（子类名必须以 Layout 结尾）。实际推导：" + detail,
                EXPECTED_KINDS, derived);
    }

    /**
     * 子类名一律以 {@code Layout} 结尾 —— 上一条断言成立的前提，单独钉一次好让失败信息
     * 直接指向病因（而不是只报「推导出了 xxx」）。
     */
    @Test
    public void subclassNamesEndWithLayoutSuffix() {
        for (Class<?> sub : LAYOUT_SUBCLASSES) {
            Assert.assertTrue(sub.getSimpleName() + " 必须以 Layout 结尾"
                            + "（前端 resolveImplKind(impl, 'Layout') 靠剥这个后缀推导短 key）",
                    sub.getSimpleName().endsWith("Layout"));
        }
    }

    // ===================================================================
    // 3. json 的存在性规则
    // ===================================================================

    /**
     * 声明了 {@code @FormField} 字段的子类<b>必须</b>有自己的 {@code .json}；
     * 零字段的子类<b>必须没有</b>。
     *
     * <p>两个方向都是静默失效：漏建 json 时字段的 label/help 全空（表单照常渲染，
     * 用户只看到一排英文字段名）；给零字段的类建了 json 则说明有人往「无参数」的布局里
     * 加了字段 —— 那正是本次改造要消灭的「布局说自己没有参数、却带着载荷」的状态。
     */
    @Test
    public void formResourceExistenceFollowsFormFields() {
        for (Class<?> sub : LAYOUT_SUBCLASSES) {
            boolean hasFormField = declaresFormField(sub);
            JSONObject resource = TisPluginFormTestSupport.formResource(sub.getName());
            if (hasFormField) {
                Assert.assertNotNull(sub.getSimpleName() + " 声明了 @FormField 字段，因此必须有同名 .json"
                        + "（缺失时表单里该字段的 label/help 全空，且没有任何报错）", resource);
                for (Field f : formFieldsOf(sub)) {
                    Assert.assertNotNull(sub.getSimpleName() + ".json 缺少字段 " + f.getName() + " 的条目",
                            resource.getJSONObject(f.getName()));
                }
            } else {
                Assert.assertNull(sub.getSimpleName() + " 是零字段的布局子类，不应有 .json"
                        + "（有 json 意味着有人往无参数的布局里加了字段）", resource);
            }
        }
    }

    /** 无字段的那三个子类与上面规则里的另一半对齐，单独钉一次好定位到具体类。 */
    @Test
    public void fieldlessSubclassesAreExactlyRowsAndToolbar() {
        List<String> actual = new ArrayList<>();
        for (Class<?> sub : LAYOUT_SUBCLASSES) {
            if (!declaresFormField(sub)) {
                actual.add(sub.getSimpleName());
            }
        }
        Assert.assertEquals("无 @FormField 字段的布局子类应恰好是 Rows / Toolbar",
                simpleNames(FIELDLESS_SUBCLASSES),
                new TreeSet<>(actual));
    }

    // ===================================================================
    // 4. 扁平字段不得复辟
    // ===================================================================

    /**
     * 改造中被合并掉的 {@code layoutConfig} 不允许再出现在 {@code WorkshopSection} 上，
     * 且 {@code layout} 的类型必须是抽象基类。
     *
     * <p>光靠注释说明不了问题：{@code layoutConfig} 一旦被加回来（哪怕是为了「兼容旧数据」），
     * 「{@code layout = ColumnsLayout} 配 {@code layoutConfig = TabsLayoutConfig}」这种
     * 矛盾态就重新可表示了，而编译器不会有任何意见。
     */
    @Test
    public void layoutConfigMustNotComeBack() {
        try {
            WorkshopSection.class.getField("layoutConfig");
            Assert.fail("WorkshopSection.layoutConfig 已合并进多态字段 layout，不得复辟"
                    + "（复辟即意味着自相矛盾的布局状态重新可表示）");
        } catch (NoSuchFieldException expected) {
            // 正是期望的结果
        }

        try {
            Assert.assertEquals("WorkshopSection.layout 的类型应是抽象基类 SectionLayout",
                    SectionLayout.class, WorkshopSection.class.getField("layout").getType());
        } catch (NoSuchFieldException e) {
            Assert.fail("WorkshopSection 缺少 layout 字段");
        }

        Assert.assertFalse("SectionLayout 应是抽象类而非枚举 —— 若它变回枚举，"
                        + "「哪种布局」与「那种布局的参数」就又分成两套分类体系了",
                SectionLayout.class.isEnum());
        Assert.assertTrue("SectionLayout 应是抽象类",
                java.lang.reflect.Modifier.isAbstract(SectionLayout.class.getModifiers()));
    }

    /**
     * {@code WorkshopSection.json} 的 {@code layout} 块不得再带 {@code enum} 数组。
     *
     * <p>那段枚举是 {@code FormFieldType.ENUM} 时代的产物。字段改成子表单后它仍然解析得到
     * （json 只是被原样下发），会诱导前端把一个 Describable 字段按枚举渲染 —— 表现为
     * 「布局变成一个只有六个字面量的下拉，而不是能展开的子表单」。两个平面的取值恰好一一对应，
     * 所以渲染出来<b>看着像对的</b>，只是子类参数（columnCount 等）再也填不了。
     */
    @Test
    public void workshopSectionJsonNoLongerDeclaresLayoutEnum() {
        // layout 等共享字段的 label/help 已随字段上收到基类，落在 AbstractWorkshopSection.json
        JSONObject resource = TisPluginFormTestSupport.formResource(AbstractWorkshopSection.class.getName());
        Assert.assertNotNull("AbstractWorkshopSection.json 应存在（layout 等共享字段 label 所在）", resource);

        JSONObject layoutMeta = resource.getJSONObject("layout");
        Assert.assertNotNull("AbstractWorkshopSection.json 应有 layout 条目", layoutMeta);
        Assert.assertNull("layout 的 enum 数组应随枚举一并删除"
                        + "（留着会让前端把子表单字段按枚举渲染，子类参数填不了）",
                layoutMeta.get("enum"));
        Assert.assertNotNull("layout 的 label 应保留", layoutMeta.getString("label"));
        Assert.assertFalse("AbstractWorkshopSection.json 不应再有 layoutConfig 条目",
                resource.containsKey("layoutConfig"));
    }

    // ===================================================================
    // 5. 运行期：impl 下拉与表单属性
    // ===================================================================

    /**
     * 六个子类在运行期真的能被枚举成一份 impl 列表 —— 也就是配置面板上那个「布局」下拉的选项来源。
     *
     * <p>上面的静态断言只能证明「类存在、注解在、FQCN 推导对」；能不能被 {@code DescriptorExtensionList}
     * 收进来是另一回事：{@code Descriptor.getT()} 走
     * {@code Types.getBaseClass(getClass(), Descriptor.class)} 沿继承链找泛型实参，
     * 一旦有人把 {@code BasicDescriptor} 写成裸 {@code Descriptor}（不带 {@code <SectionLayout>}），
     * 子类就会落到别的列表里、或直接抛 {@code IllegalStateException} —— 两种都不报编译错。
     *
     * <p>需要 TIS 运行期，环境不具备时整条跳过（类内其余断言不受影响）。
     */
    @Test
    public void sectionLayoutIsEnumerableAsImplList() {
        Assume.assumeTrue(RUNTIME_ASSUME_MSG, tisRuntimeReachable());

        Collection<Descriptor<SectionLayout>> list = TIS.get().getDescriptorList(SectionLayout.class);
        Assert.assertEquals("SectionLayout 的 descriptor 列表应恰好六项 —— 这就是前端 impl 下拉的选项来源"
                        + "（少一项＝某种布局选不到；漏 @TISExtension 时不报错，只是那项不见了）",
                LAYOUT_SUBCLASSES.size(), list.size());

        for (Descriptor<SectionLayout> d : list) {
            Assert.assertEquals("descriptor 的泛型实参必须是 SectionLayout 本身"
                            + "（getT() 决定它落进哪个 DescriptorExtensionList）",
                    SectionLayout.class, d.getT());
            Assert.assertNotNull("descriptor 的 displayName 不得为空（它是下拉项的标题）", d.getDisplayName());
        }

        Set<String> ids = new TreeSet<>();
        for (Descriptor<SectionLayout> d : list) {
            ids.add(d.getId());
        }
        Assert.assertEquals("运行期枚举到的 descriptor id 应与六个子类的 FQCN 逐字相同",
                fqcns(LAYOUT_SUBCLASSES), ids);
    }

    /**
     * {@code layout} 在表单层被当作<b>子表单</b>而不是枚举。
     *
     * <p>这是本次改造在表单上的最终判据。{@code @FormField} 上没写 {@code type}，
     * 由框架按字段的实际类型（{@code SectionLayout} 是 {@code Describable}）判定 ——
     * 也就是说这条不是「配置对不对」而是「框架认没认出来」。认不出来的表现是前端收到一个
     * 枚举字段：六个字面量恰好一一对应，<b>看着像对的</b>，但子类的参数（{@code columnCount} 等）
     * 永远填不了。
     */
    @Test
    public void layoutFieldRendersAsSubFormNotEnum() {
        Assume.assumeTrue(RUNTIME_ASSUME_MSG, tisRuntimeReachable());

        Descriptor<WorkshopSection> desc = TIS.get().getDescriptor(WorkshopSection.class);
        Map<String, IPropertyType> props =
                PropertyType.buildPropertyTypes(ElementPluginDesc.create(desc), WorkshopSection.class);

        PropertyType layout = (PropertyType) props.get("layout");
        Assert.assertNotNull("WorkshopSection 的表单属性里应有 layout", layout);
        Assert.assertEquals("layout 的字段类型应是抽象基类 SectionLayout",
                SectionLayout.class, layout.fieldClazz);
        Assert.assertNull("layout 的 extraProps 不得再带 enum 数组"
                        + "（带着它前端会把子表单字段按枚举渲染，子类参数填不了）",
                layout.getExtraProps().getJSONArray(Descriptor.KEY_ENUM_PROP));

        Assert.assertFalse("layoutConfig 不应再是 WorkshopSection 的表单属性"
                + "（它已合并进 layout）", props.containsKey("layoutConfig"));

        // 子类的字段属于各自子表单的属性集，不得被并进宿主类这一层。
        // 若被并进来，说明 WorkshopSection 的 ordinal 0..5 与各 *Layout 子类的 0/1
        // 落在了同一张 PropertyType 表里 —— 那是「基类不得声明 @FormField」这条规矩
        // 要防的同一类顺序漂移，只是换了个来路（合并前是 SectionLayoutConfig 的字段）。
        // id 是 AbstractWorkshopSection 上的共享字段（identity，表单里 readonly 展示），
        // 随基类进入本层属性表，不是某个 *Layout 子类并进来的。
        Assert.assertEquals("WorkshopSection 的表单属性应恰好是它自己的八个字段"
                        + "（七个共享字段 + childSections），不得混入 ColumnsLayout / TabsLayout 等子类的字段",
                new TreeSet<>(Arrays.asList("id", "name", "layout", "styleConfig",
                        "conditionalVisibility", "dropHandling", "sortOrder", "childSections")),
                new TreeSet<>(props.keySet()));
    }

    // ===================================================================
    //  helpers
    // ===================================================================

    private static final String RUNTIME_ASSUME_MSG =
            "当前环境未初始化 TIS（JDK17 下通常还缺 --add-opens java.base/java.lang=ALL-UNNAMED），"
                    + "跳过运行期检查；类内其余断言不依赖它";

    /**
     * TIS 的 descriptor 扫描是否可用。
     *
     * <p>{@code DescriptorExtensionList} 是惰性的：真正的扩展扫描发生在迭代上，
     * 所以必须在这个 try 里就把它物化，否则异常会漏到外面变成失败而非跳过。
     */
    private static boolean tisRuntimeReachable() {
        try {
            TIS tis = TIS.get();
            return tis != null && !tis.getDescriptorList(SectionLayout.class).isEmpty();
        } catch (Throwable t) {
            return false;
        }
    }

    private static Set<String> fqcns(List<Class<?>> classes) {
        return classes.stream().map(Class::getName)
                .collect(java.util.stream.Collectors.toCollection(TreeSet::new));
    }

    /**
     * 抄自前端 {@code click-action.model.ts} 的 {@code resolveImplKind}，<b>逐条同规则</b>：
     * 取末段 → 剥后缀 → 两个大小写边界各插一个分隔符 → 转小写。
     *
     * <p>放在测试里是刻意的：这样「前端以为的推导规则」和「后端的实际类名」在同一个用例里
     * 相遇。若前端那份改了而这里没跟上，这条断言会红 —— 反过来也是。
     */
    private static String resolveImplKind(String impl, String stripSuffix) {
        if (impl == null || impl.isEmpty()) {
            return "";
        }
        if (impl.indexOf('.') < 0) {
            return impl;
        }
        String simpleName = impl.substring(impl.lastIndexOf('.') + 1);
        String name = (stripSuffix != null && !stripSuffix.isEmpty() && simpleName.endsWith(stripSuffix))
                ? simpleName.substring(0, simpleName.length() - stripSuffix.length())
                : simpleName;
        return name
                // 小写/数字 后紧跟大写 → 插分隔符：OpenOverlay → Open-Overlay
                .replaceAll("([a-z0-9])([A-Z])", "$1-$2")
                // 连续大写 后紧跟 大写+小写 → 在缩写边界插分隔符：SQLQuery → SQL-Query
                .replaceAll("([A-Z]+)([A-Z][a-z])", "$1-$2")
                .toLowerCase();
    }

    /**
     * {@code Class} 不是 {@code Comparable}，故不能直接 {@code new TreeSet<>(listOfClasses)}
     *（会抛 ClassCastException，且是在构造器里抛、看不出是测试自身的写法问题）。
     * 比的是<b>简单类名</b>，与 {@code TisPluginFormTestSupport.names()} 的口径一致。
     */
    private static Set<String> simpleNames(List<Class<?>> classes) {
        return classes.stream().map(Class::getSimpleName)
                .collect(java.util.stream.Collectors.toCollection(TreeSet::new));
    }

    private static boolean declaresFormField(Class<?> clazz) {
        return !formFieldsOf(clazz).isEmpty();
    }

    private static List<Field> formFieldsOf(Class<?> clazz) {
        List<Field> result = new ArrayList<>();
        for (Field f : clazz.getDeclaredFields()) {
            if (f.getAnnotation(FormField.class) != null) {
                result.add(f);
            }
        }
        return result;
    }
}
