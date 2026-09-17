package com.qlangtech.tis.plugin.ontology.workshop.model.config;

import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.plugin.ds.ElementCreatorFactory;
import com.qlangtech.tis.plugin.ds.ViewContent;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * {@code WorkshopVariable} 三个「开关型」聚合属性插件改造的回归测试。
 *
 * <p>本轮把 {@code interfaceConfig} / {@code routingConfig} / {@code stateSavingConfig}
 * 从「普通 POJO + {@code boolean enabled}」改成「抽象基类 + 开启子类 + 关闭子类」——
 * 开关由 <b>Java 类型</b>承担。这类改造有三条<b>没有任何编译期保护</b>的接线，
 * 全部由本类钉住：
 *
 * <ol>
 *   <li><b>子类形态</b> —— 每个基类恰好两个子类，其中恰好一个的
 *       {@code getDisplayName()} 返回 {@link Descriptor#SWITCH_OFF}</li>
 *   <li><b>默认项接线</b> —— {@code WorkshopVariable.json} 三个字段的 {@code dftVal}
 *       必须是 {@code "off"}。它钉住的是一条<b>跨语言约定</b>：前端
 *       {@code tis.plugin.ts} 拿 {@code dftVal} 去逐项比对子类的 {@code displayName}
 *       来选默认项，Java 侧改个字符串就会静默丢掉默认值</li>
 *   <li><b>行编辑器契约</b> —— 两个 {@code MULTI_SELECTABLE} 字段的
 *       {@code elementCreator} 能反射实例化、{@code getTuplesKey()} 非空且互不相同、
 *       {@code createDefault()} 不返回 null</li>
 *   <li><b>线格式键名（跨工程）</b> —— {@code ViewContent} 的 token 与前端
 *       {@code TuplesPropertyType} 的字符串、{@code getTuplesKey()} 与前端属性类挂行数组
 *       的字段名必须逐字一致。写错<b>不会报错</b>，只会静默解析出 0 行，故这里跨工程
 *       直接读前端源码核对</li>
 * </ol>
 *
 * <p>与 {@code TestWidgetVariableContract} 同路数：<b>不依赖 TIS 运行期</b>
 * （不需要 {@code --add-opens}，也不需要在 {@code @Before} 里 Assume 跳过）。
 * 类清单来自编译期生成的 sezpoz 注解索引，资源靠 classloader 读，descriptor 直接
 * {@code newInstance}。这样本类在默认 surefire 配置下就能跑，不会因为环境问题变成
 * 「一直绿但什么都没查」。
 *
 * <p>第 3 条里的 {@code elementCreator} + {@code enum} 两键本身已由
 * {@code TestWidgetVariableContract#everyMultiSelectableFieldHasRowEditor} 覆盖
 * （其扫描范围含 {@code workshop.model.config}），此处不重复。
 */
public class TestWorkshopConfigPlugins {

    /**
     * sezpoz 在编译期为 {@code @TISExtension} 生成的扩展点索引 —— TIS 的 ExtensionFinder
     * 实际读的就是它（{@code @TISExtension} 是 {@code RetentionPolicy.CLASS}，
     * 运行期 {@code getAnnotation} 恒为 null，不能直接查注解）。
     */
    private static final String SEZPOZ_INDEX_PATH =
            "/META-INF/annotations/com.qlangtech.tis.extension.TISExtension.txt";

    /** 本轮改造所在的包 */
    private static final String CONFIG_PKG = "com.qlangtech.tis.plugin.ontology.workshop.model.config.";

    private static final String FIELD_INTERFACE_CONFIG = "interfaceConfig";
    private static final String FIELD_ROUTING_CONFIG = "routingConfig";
    private static final String FIELD_STATE_SAVING_CONFIG = "stateSavingConfig";

    /** 三个基类 -> 期望的「开启」子类；「关闭」子类由 displayName == SWITCH_OFF 识别 */
    private static final Map<Class<?>, Class<?>> BASES = new LinkedHashMap<>();

    static {
        BASES.put(VariableInterfaceConfig.class, MappingInterfaceConfig.class);
        BASES.put(VariableRoutingConfig.class, PageRoutingConfig.class);
        BASES.put(VariableStateSavingConfig.class, PersistentStateSavingConfig.class);
    }

    /**
     * 「开启」子类 -> 它那个 {@code MULTI_SELECTABLE} 字段名。
     *
     * <p>{@code elementCreator} 的 FQCN 从各子类的 {@code .json} 里读（那是唯一事实源），
     * 期望的前端键名则记在这里 —— 后者在前端代码里，只能跨工程核对。
     */
    private static final Map<Class<?>, String[]> ROW_EDITOR_FIELDS = new LinkedHashMap<>();

    static {
        ROW_EDITOR_FIELDS.put(MappingInterfaceConfig.class,
                new String[]{"inputs", "interfaceInputRows", "_interfaceInputs", "接口参数名"});
        ROW_EDITOR_FIELDS.put(PageRoutingConfig.class,
                new String[]{"params", "routingParamRows", "_routingParams", "URL 参数名"});
    }

    /** 前端工程根目录（CLAUDE.md：所有前端代码在 tis-console） */
    private static final Path FRONTEND_ROOT = Paths.get("../../tis-console");

    // ===================================================================
    // 1. 每个基类恰好两个子类，其中恰好一个 displayName == "off"
    // ===================================================================

    /**
     * 三个基类的具体子类集合必须<b>双向相等</b>，即「开启子类 + 一个关闭子类」。
     *
     * <p>用双向相等而非「扫到的 ⊆ 期望」：后者在扫到 0 个时循环体不执行，会假通过。
     * 这条同时是「多了一种开关形态」的哨兵 —— 加子类必须同时更新本测试与前端。
     */
    @Test
    public void everyBaseHasExactlyTwoSubclassesOneOfWhichIsOff() {
        for (Map.Entry<Class<?>, Class<?>> e : BASES.entrySet()) {
            Class<?> base = e.getKey();
            Set<Class<?>> actual = concreteSubclassesOf(base);

            Set<String> expected = new TreeSet<>(Arrays.asList(
                    e.getValue().getSimpleName(), offSubclassOf(base).getSimpleName()));
            Assert.assertEquals(base.getSimpleName() + " 应恰好有两个具体子类（一个开启、一个关闭），"
                            + "实际：" + names(actual), expected, names(actual));

            // 「关闭」的判据不是类名里有 None，而是它的 displayName 就是 SWITCH_OFF ——
            // 前端选默认项比对的正是这个字符串，名字对了但 displayName 写错同样是坏的
            List<String> offs = new ArrayList<>();
            for (Class<?> sub : actual) {
                if (Descriptor.SWITCH_OFF.equals(descriptorOf(sub).getDisplayName())) {
                    offs.add(sub.getSimpleName());
                }
            }
            Assert.assertEquals(base.getSimpleName() + " 应恰好有一个子类的 displayName 是 "
                    + "SWITCH_OFF（前端按它选默认项），实际是：" + offs, 1, offs.size());
        }
    }

    /**
     * descriptor 的 {@code clazz} 必须落在具体子类上 —— {@code getId()} 返回的就是它，
     * 也是前端在 impl 下拉里区分「开启 / 关闭」的依据。
     *
     * <p>{@code Descriptor()} 无参构造把 {@code clazz} 置为 {@code getClass().getEnclosingClass()}，
     * 所以只要 descriptor 嵌套在具体子类里就自然成立；一旦有人图省事写成
     * {@code super(VariableInterfaceConfig.class)}（或把 descriptor 提到基类上），
     * 两个子类的 {@code getId()} 就会撞成同一个，前端分辨不出开关。
     */
    @Test
    public void descriptorsIdentifyTheirConcreteSubclass() {
        for (Class<?> onSubclass : BASES.values()) {
            Descriptor<?> desc = descriptorOf(onSubclass);
            Assert.assertEquals(onSubclass.getSimpleName() + " 的 descriptor.getId() 应等于其自身 FQCN"
                            + "（两个子类撞成同一个 id 时前端无法区分开关）",
                    onSubclass.getName(), desc.getId());
        }
    }

    // ===================================================================
    // 2. 默认项接线（跨语言约定）
    // ===================================================================

    /**
     * {@code WorkshopVariable.json} 里三个开关字段的 {@code dftVal} 必须是 {@code "off"}。
     *
     * <p>这条没有任何编译期保护：前端拿 {@code dftVal} 去逐项比对子类的
     * {@code displayName} 来选默认项，值写错的表现是「默认落到了某个开启子类上」，
     * 一行日志都没有。
     */
    @Test
    public void switchFieldsDefaultToOffInFormResource() {
        JSONObject resource = formResource(
                "com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopVariable");
        Assert.assertNotNull("WorkshopVariable.json 应存在", resource);

        for (String fieldName : new String[]{FIELD_INTERFACE_CONFIG, FIELD_ROUTING_CONFIG,
                FIELD_STATE_SAVING_CONFIG}) {
            JSONObject fieldMeta = resource.getJSONObject(fieldName);
            Assert.assertNotNull(fieldName + " 在 WorkshopVariable.json 里应有条目", fieldMeta);
            Assert.assertEquals(fieldName + " 的默认项应为开关的关闭态（dftVal 必须逐字等于 "
                            + "Descriptor.SWITCH_OFF，前端按它匹配子类的 displayName）",
                    Descriptor.SWITCH_OFF, fieldMeta.getString("dftVal"));
        }
    }

    // ===================================================================
    // 3. 行编辑器契约
    // ===================================================================

    /**
     * 两个 {@code MULTI_SELECTABLE} 字段的 {@code elementCreator}（FQCN 从各子类
     * {@code .json} 里读）必须：
     * ①有无参构造（框架走 {@code loadClass(FQCN).newInstance()}）；
     * ②{@code getTuplesKey()} 非空且两者互不相同；
     * ③{@code createDefault(new JSONObject())} 不返回 null
     * （{@code getElementPropertyKeys()} 会 BeanUtils 内省它的结果）。
     */
    @Test
    public void rowEditorFactoriesAreInstantiable() throws Exception {
        Set<String> tuplesKeys = new TreeSet<>();
        Set<String> viewTokens = new TreeSet<>();

        for (Map.Entry<Class<?>, String[]> e : ROW_EDITOR_FIELDS.entrySet()) {
            ElementCreatorFactory<?> creator = readElementCreator(e.getKey(), e.getValue()[0]);

            String tuplesKey = creator.getTuplesKey();
            Assert.assertFalse(e.getKey().getSimpleName() + " 的 getTuplesKey() 不得为空"
                            + "（前端按它挂行数组，前端侧见本类 ROW_EDITOR_FIELDS 的第三个值）",
                    tuplesKey == null || tuplesKey.trim().isEmpty());
            Assert.assertTrue("两个行编辑器的 getTuplesKey() 不得相同：" + tuplesKey,
                    tuplesKeys.add(tuplesKey));

            ViewContent viewType = creator.getViewContentType();
            Assert.assertNotNull(e.getKey().getSimpleName() + " 的 getViewContentType() 不得为 null",
                    viewType);
            Assert.assertTrue("两个行编辑器的 ViewContent token 不得相同：" + viewType.getToken(),
                    viewTokens.add(viewType.getToken()));

            Assert.assertNotNull(e.getKey().getSimpleName()
                            + " 的 createDefault() 不得返回 null（框架会内省它的 JavaBean 属性）",
                    creator.createDefault(new JSONObject()));
        }

        Assert.assertEquals("两个行编辑器都应有 tuplesKey", ROW_EDITOR_FIELDS.size(), tuplesKeys.size());
    }

    // ===================================================================
    // 4. 线格式键名（跨工程核对前端）
    // ===================================================================

    /**
     * 三个跨工程的键名约定必须逐字一致，否则<b>静默失效</b>：
     *
     * <ul>
     *   <li>{@code ViewContent.getToken()} ↔ 前端 {@code TuplesPropertyType} 的同名成员
     *       —— 对不上时前端 {@code buildMultiSelectedAttr} 的 switch 落到
     *       {@code default} 分支直接 throw，整个表单打不开</li>
     *   <li>{@code ElementCreatorFactory.getTuplesKey()} ↔ 前端属性类挂行数组的字段名
     *       —— 对不上时服务端解析出 <b>0 行</b>，无任何报错</li>
     * </ul>
     *
     * <p>前端工程不在场时跳过（它不是本仓库的构建依赖），在场时就是一条真的跨语言契约检查。
     */
    @Test
    public void rowEditorWireFormatKeysMatchFrontend() throws Exception {
        Path pluginTs = FRONTEND_ROOT.resolve("src/common/tis.plugin.ts");
        Path ontologyCommonTs = FRONTEND_ROOT.resolve("src/base/common/ontology.common.ts");
        org.junit.Assume.assumeTrue("前端工程 " + FRONTEND_ROOT.toAbsolutePath().normalize()
                        + " 不在场，跳过跨工程键名核对",
                Files.isRegularFile(pluginTs) && Files.isRegularFile(ontologyCommonTs));

        String pluginTsText = Files.readString(pluginTs);
        String ontologyCommonTsText = Files.readString(ontologyCommonTs);

        List<String> problems = new ArrayList<>();
        for (Map.Entry<Class<?>, String[]> e : ROW_EDITOR_FIELDS.entrySet()) {
            ElementCreatorFactory<?> creator = readElementCreator(e.getKey(), e.getValue()[0]);
            String viewToken = creator.getViewContentType().getToken();
            String tuplesKey = creator.getTuplesKey();
            String frontendToken = e.getValue()[1];
            String frontendRowsKey = e.getValue()[2];
            String keyColumnTitle = e.getValue()[3];

            if (!frontendToken.equals(viewToken)) {
                problems.add(e.getKey().getSimpleName() + " 的 ViewContent token 是 " + viewToken
                        + "，本测试登记的前端 token 是 " + frontendToken + "（登记过期？）");
            }
            if (!pluginTsText.contains("= (\"" + viewToken + "\")")
                    && !pluginTsText.contains("= ('" + viewToken + "')")) {
                problems.add("前端 tis.plugin.ts 的 TuplesPropertyType 里没有 token " + viewToken
                        + "（buildMultiSelectedAttr 会落到 default 分支抛错，整张表单打不开）");
            }
            if (!frontendRowsKey.equals(tuplesKey)) {
                problems.add(e.getKey().getSimpleName() + " 的 tuplesKey 是 " + tuplesKey
                        + "，本测试登记的前端行数组键是 " + frontendRowsKey + "（登记过期？）");
            }
            if (!ontologyCommonTsText.contains(frontendRowsKey)) {
                problems.add("前端 ontology.common.ts 里没有行数组键 " + frontendRowsKey
                        + "（服务端将静默解析出 0 行）");
            }
            if (!ontologyCommonTsText.contains(keyColumnTitle)) {
                problems.add("前端 ontology.common.ts 里没有列头文案 " + keyColumnTitle
                        + "（列头是本测试登记的，写错只是显示不好看，一并钉住）");
            }
        }

        Assert.assertTrue(String.join("\n", problems), problems.isEmpty());
    }

    // ===================================================================
    //  helpers
    // ===================================================================

    /** 与 {@code None} 前缀约定对应的关闭子类 */
    private static Class<?> offSubclassOf(Class<?> base) {
        for (Class<?> sub : concreteSubclassesOf(base)) {
            if (sub.getSimpleName().startsWith("None")) {
                return sub;
            }
        }
        throw new AssertionError(base.getSimpleName() + " 下没有以 None 开头的关闭子类");
    }

    /**
     * sezpoz 索引里 {@code config} 包下的具体子类 —— 与 {@code base} 可赋值者。
     *
     * <p>从索引枚举而非硬编码类名清单：新增子类只要带 {@code @TISExtension}
     * 就自动进入覆盖范围，不会因为测试忘了同步而漏检。
     */
    private static Set<Class<?>> concreteSubclassesOf(Class<?> base) {
        Set<Class<?>> result = new TreeSet<>(java.util.Comparator.comparing(Class::getName));
        for (Class<?> clazz : configPackageClasses()) {
            if (Modifier.isAbstract(clazz.getModifiers()) || base.equals(clazz)) {
                continue;
            }
            if (base.isAssignableFrom(clazz)) {
                result.add(clazz);
            }
        }
        Assert.assertFalse(base.getSimpleName() + " 在 sezpoz 索引里一个具体子类都没扫到"
                + "（注解处理器未生效？扫描包写错了？）", result.isEmpty());
        return result;
    }

    /** sezpoz 索引中 {@code config} 包下的类（已加载，按名字排序，保证失败信息稳定） */
    private static List<Class<?>> configPackageClasses() {
        List<Class<?>> result = new ArrayList<>();
        for (String fqcn : sezpozIndexEntries()) {
            if (!fqcn.startsWith(CONFIG_PKG)) {
                continue;
            }
            Class<?> clazz = outerClassOf(fqcn);
            if (clazz != null && result.stream().noneMatch(c -> c == clazz)) {
                result.add(clazz);
            }
        }
        Assert.assertFalse("sezpoz 索引中 " + CONFIG_PKG + " 下没有任何条目", result.isEmpty());
        return result;
    }

    /** 索引条目所在的外部类；已是顶层类则返回自身，加载不到返回 null */
    private static Class<?> outerClassOf(String fqcn) {
        try {
            Class<?> clazz = Class.forName(fqcn, false, TestWorkshopConfigPlugins.class.getClassLoader());
            Class<?> outer = clazz.getEnclosingClass();
            return outer != null ? outer : clazz;
        } catch (Throwable t) {
            return null;
        }
    }

    /** 类的嵌套 descriptor（约定名 {@code DefaultDescriptor} 优先，否则取第一个） */
    private static Descriptor<?> descriptorOf(Class<?> pluginClass) {
        Descriptor<?> fallback = null;
        for (Class<?> nested : pluginClass.getDeclaredClasses()) {
            if (!Descriptor.class.isAssignableFrom(nested)) {
                continue;
            }
            Descriptor<?> desc = newDescriptor(nested);
            if (desc == null) {
                continue;
            }
            if ("DefaultDescriptor".equals(nested.getSimpleName())) {
                return desc;
            }
            if (fallback == null) {
                fallback = desc;
            }
        }
        Assert.assertNotNull(pluginClass.getSimpleName()
                + " 没有可实例化的嵌套 descriptor（@TISExtension 类缺失或构造抛异常？）", fallback);
        return fallback;
    }

    private static Descriptor<?> newDescriptor(Class<?> descriptorClass) {
        try {
            return (Descriptor<?>) descriptorClass.getDeclaredConstructor().newInstance();
        } catch (Throwable t) {
            return null;
        }
    }

    /**
     * 从子类的 {@code .json} 读出该字段的 {@code elementCreator} 并反射实例化。
     *
     * <p>刻意从 json 读而非写死类名 —— 写死的话，json 里的 FQCN 改错时本测试仍会绿。
     */
    private static ElementCreatorFactory<?> readElementCreator(Class<?> pluginClass, String fieldName)
            throws Exception {
        JSONObject resource = formResource(pluginClass.getName());
        Assert.assertNotNull(pluginClass.getSimpleName() + ".json 应存在", resource);

        JSONObject fieldMeta = resource.getJSONObject(fieldName);
        Assert.assertNotNull(pluginClass.getSimpleName() + ".json 里应有 " + fieldName + " 条目",
                fieldMeta);

        String creatorFqcn = fieldMeta.getString("elementCreator");
        Assert.assertNotNull(pluginClass.getSimpleName() + " 的 " + fieldName
                + " 缺少 elementCreator（前端 buildMultiSelectedAttr 会直接抛错）", creatorFqcn);

        Class<?> creatorClass = Class.forName(creatorFqcn, true,
                TestWorkshopConfigPlugins.class.getClassLoader());
        Assert.assertTrue(creatorFqcn + " 应实现 ElementCreatorFactory",
                ElementCreatorFactory.class.isAssignableFrom(creatorClass));
        // 框架走 loadClass(FQCN).newInstance()，无参构造是硬性要求
        return (ElementCreatorFactory<?>) creatorClass.getDeclaredConstructor().newInstance();
    }

    /** 表单资源路径：{@code /<FQCN 的点号换成斜杠>.json}；不存在返回 null */
    private static JSONObject formResource(String fqcn) {
        String path = "/" + fqcn.replace('.', '/') + ".json";
        try (InputStream in = TestWorkshopConfigPlugins.class.getResourceAsStream(path)) {
            if (in == null) {
                return null;
            }
            return JSONObject.parseObject(new String(in.readAllBytes(), StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new AssertionError("读取表单资源失败：" + path, e);
        }
    }

    private static Set<String> names(Set<Class<?>> classes) {
        return classes.stream().map(Class::getSimpleName).collect(Collectors.toCollection(TreeSet::new));
    }

    private static List<String> sezpozIndexEntries() {
        try (InputStream in = TestWorkshopConfigPlugins.class.getResourceAsStream(SEZPOZ_INDEX_PATH)) {
            Assert.assertNotNull("缺少 sezpoz 索引 " + SEZPOZ_INDEX_PATH + "（注解处理器未生效？）", in);
            String text = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            List<String> entries = new ArrayList<>();
            for (String line : text.split("\n")) {
                String trimmed = line.trim();
                if (!trimmed.isEmpty()) {
                    entries.add(trimmed);
                }
            }
            return entries;
        } catch (IOException e) {
            throw new AssertionError("读取 sezpoz 索引失败：" + SEZPOZ_INDEX_PATH, e);
        }
    }
}
