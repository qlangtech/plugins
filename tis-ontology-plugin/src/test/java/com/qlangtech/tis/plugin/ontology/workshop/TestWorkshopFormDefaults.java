package com.qlangtech.tis.plugin.ontology.workshop;

import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.plugin.annotation.FormField;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 「Java 字段初值必须同步写到 .json 的 dftVal」这条跨语言约定的守卫测试。
 *
 * <h3>为什么需要它</h3>
 * 前端新建表单的初值<b>只</b>来自 {@code eprops.dftVal}（{@code tis.plugin.ts} 的
 * {@code ItemPropVal.primary} / {@code addNewEmptyItemProp}），Java 字段初始化器它一概不读。
 * 所以 {@code public Boolean showLabels = false;} 这种写法在表单上等于「什么都没设」：
 * 用户打开表单看到空控件，必填项还会因此卡住保存。两边都不报错，只能靠测试兜。
 *
 * <h3>校验口径</h3>
 * 反射读不到初始化器的<b>表达式</b>，但读得到它的<b>结果</b>：{@code newInstance()} 之后再取字段值，
 * 拿到的就是初始化器（或构造函数）算出来的有效初值。于是本用例对每个可实例化、带
 * {@code @FormField} 的字段做三件事：
 * <ol>
 *   <li>取运行期初值；{@code null} 视为「作者没打算给默认值」，跳过</li>
 *   <li>按值类型归一成 {@code dftVal} 的线格式（见 {@link #expectedDftVal}）</li>
 *   <li>与「本类 + 全部祖先」逐层深合并后的 {@code .json} 比对 —— 与
 *       {@code PluginExtraProps.load()} 的 {@code visitAncestorsClass} 同序，子类覆盖父类</li>
 * </ol>
 *
 * <p>不需要 TIS 运行期，也不碰 sezpoz 的 descriptor 扫描之外的框架代码，因此默认 surefire
 * 配置下就能真跑（不会退化成 Assume 跳过）。
 */
public class TestWorkshopFormDefaults {

    private static final String PKG_ROOT = "com.qlangtech.tis.plugin.ontology.workshop";

    @Test
    public void everyJavaInitializerHasMatchingDftVal() {
        List<String> problems = new ArrayList<>();
        int checked = 0;
        int classes = 0;

        for (Class<?> clazz : workshopClasses()) {
            Object instance;
            try {
                instance = clazz.getDeclaredConstructor().newInstance();
            } catch (Throwable t) {
                // 没有无参构造器 / 构造依赖运行期上下文 → 读不到初值，只能跳过（不影响别的类）
                continue;
            }
            classes++;
            Map<String, JSONObject> merged = mergedFormResource(clazz);
            for (Field f : clazz.getDeclaredFields()) {
                FormField ff = f.getAnnotation(FormField.class);
                if (ff == null || Modifier.isStatic(f.getModifiers()) || Modifier.isTransient(f.getModifiers())) {
                    continue;
                }
                // 列表 / 映射没有标量初值可比，元组行编辑器另一套线格式
                if (Collection.class.isAssignableFrom(f.getType()) || Map.class.isAssignableFrom(f.getType())) {
                    continue;
                }
                Object val;
                try {
                    f.setAccessible(true);
                    val = f.get(instance);
                } catch (Throwable t) {
                    continue;
                }
                if (val == null) {
                    continue;
                }
                Object expected = expectedDftVal(val);
                if (expected == null) {
                    continue;
                }
                String where = clazz.getSimpleName() + "." + f.getName();
                JSONObject props = merged.get(f.getName());
                Object actual = props == null ? null : props.get("dftVal");
                if (actual == null) {
                    problems.add(where + " 有 Java 初值 " + describe(val) + "，但 .json 缺 dftVal"
                            + " —— 新建表单上该字段是空的");
                    continue;
                }
                if (!sameValue(expected, actual)) {
                    problems.add(where + " 的 dftVal 与 Java 初值不一致：json=" + describe(actual)
                            + "，Java=" + describe(val) + "（期望 dftVal 写成 " + describe(expected) + "）");
                    continue;
                }
                checked++;
            }
        }

        Assert.assertTrue("在 " + classes + " 个可实例化的 workshop 类里，只有 " + checked
                + " 个字段比中了 dftVal —— 扫描范围写错或所有类都实例化失败了？", checked >= 60);
        Assert.assertTrue("发现 " + problems.size() + " 处「Java 有初值、json 无对应 dftVal」：\n  "
                + String.join("\n  ", problems), problems.isEmpty());
    }

    /**
     * 每个 workshop 下的 {@code .json} 都必须能在<b>同名同包</b>找到 java 类。
     *
     * <p>框架取资源用的是 {@code pluginClazz.getResourceAsStream(SimpleName + ".json")}
     * （{@code PluginExtraProps:166}），即以类的包为基准的相对路径。把 json 放错一层目录
     * <b>不会报错</b>，只是它永远读不到 —— 字段的中文 label / help / dftVal 全部静默消失，
     * 表单退化成一堆英文字段名。{@code model/EventConfig.json} 就是这么躺了很久的
     * （java 在 {@code model/event/} 下）。
     *
     * <p>只做「json → java」这个方向：反方向（有 java 无 json）是合法的，很多类本就不需要资源。
     */
    @Test
    public void everyJsonResourceHasSiblingJavaClass() {
        Path projectRoot = projectRoot();
        Path resourceRoot = projectRoot.resolve("src/main/resources/" + PKG_ROOT.replace('.', '/'));
        Path javaRoot = projectRoot.resolve("src/main/java");
        Assert.assertTrue("找不到资源目录 " + resourceRoot, Files.isDirectory(resourceRoot));
        Assert.assertTrue("找不到源码目录 " + javaRoot, Files.isDirectory(javaRoot));
        List<String> orphans = new ArrayList<>();
        try (Stream<Path> paths = Files.walk(resourceRoot)) {
            for (Path p : paths.filter(Files::isRegularFile)
                    .filter(f -> f.getFileName().toString().endsWith(".json"))
                    .sorted()
                    .collect(Collectors.toList())) {
                String rel = resourceRoot.relativize(p).toString().replace(File.separatorChar, '.');
                String fqcn = PKG_ROOT + "." + rel.substring(0, rel.length() - ".json".length());
                Path javaSrc = javaRoot.resolve(fqcn.replace('.', File.separatorChar) + ".java");
                if (!Files.exists(javaSrc)) {
                    orphans.add(rel + "  ——  期望的同包类 " + fqcn + " 不存在"
                            + "（框架按「类的包」找 json，放错目录等于没写）");
                }
            }
        } catch (IOException e) {
            throw new AssertionError("遍历表单资源目录失败：" + resourceRoot, e);
        }
        Assert.assertTrue("发现 " + orphans.size() + " 个放错目录的 .json：\n  " + String.join("\n  ", orphans),
                orphans.isEmpty());
    }

    /**
     * 工程根目录（含 {@code pom.xml} 的那一层）。
     *
     * <p>本用例要比对的是<b>源码树</b>（json 与 .java 是否同包），只能落到文件系统上做，
     * 所以必须先定位工程根。取「本测试类自己的 code source」（{@code target/test-classes}）
     * 上溯到 {@code pom.xml}，比 {@code user.dir} 可靠 —— surefire 的 cwd 是模块目录，
     * 但 IDE 里跑时 cwd 常是仓库根甚至更上层。code source 不是目录（打成了 jar）时
     * 再回退到 {@code user.dir}。
     */
    private static Path projectRoot() {
        List<Path> seeds = new ArrayList<>();
        try {
            URL cs = TisPluginFormTestSupport.class.getProtectionDomain().getCodeSource().getLocation();
            seeds.add(Paths.get(cs.toURI()));
        } catch (Throwable ignore) {
            // 落到下面的 user.dir 兜底
        }
        seeds.add(Paths.get(System.getProperty("user.dir")));
        for (Path seed : seeds) {
            for (Path dir = seed; dir != null; dir = dir.getParent()) {
                if (Files.isRegularFile(dir.resolve("pom.xml"))
                        && Files.isDirectory(dir.resolve("src/main/java"))
                        && Files.isDirectory(dir.resolve("src/main/resources"))) {
                    return dir;
                }
            }
        }
        throw new AssertionError("从 " + seeds + " 上溯都找不到工程根（需同时有 pom.xml / "
                + "src/main/java / src/main/resources），本用例的目录约定失效了");
    }

    /**
     * 运行期初值 → {@code dftVal} 的线格式。返回 {@code null} 表示该值不参与本约定。
     *
     * <p>三种形态各自不同，混用都不会在运行期报错：
     * <ul>
     *   <li>枚举 → <b>常量名</b>（不是 {@code shortComment()}，也不是 json {@code enum[].label}）</li>
     *   <li>Boolean → <b>真布尔</b>（{@code Boolean} 字段的 ENUM 选项由
     *       {@code OverwriteProps.ENUM_BOOLEAN} 构造，值是 {@code true}/{@code false}，
     *       写成字符串 {@code "false"} 匹配不上）</li>
     *   <li>多态 Describable → 默认实现的 {@code Descriptor#getDisplayName()}，逐字相等</li>
     * </ul>
     */
    private static Object expectedDftVal(Object val) {
        if (val instanceof Enum<?> e) {
            return e.name();
        }
        if (val instanceof Boolean || val instanceof Number || val instanceof String) {
            return val;
        }
        if (val instanceof Describable) {
            try {
                return TisPluginFormTestSupport.descriptorOf(val.getClass()).getDisplayName();
            } catch (Throwable t) {
                // 嵌套 descriptor 无法脱离运行期实例化（依赖 IPluginContext 等）→ 本字段不参与比对
                return null;
            }
        }
        return null;
    }

    /** 数值统一按 long 比，避免 json 的 Integer 与 Java 的 Long 误报 */
    private static boolean sameValue(Object expected, Object actual) {
        if (expected instanceof Number && actual instanceof Number) {
            return ((Number) expected).longValue() == ((Number) actual).longValue();
        }
        return expected.equals(actual);
    }

    private static String describe(Object v) {
        if (v == null) {
            return "null";
        }
        if (v instanceof String) {
            return "\"" + v + "\"";
        }
        if (v instanceof Number || v instanceof Boolean) {
            return String.valueOf(v);
        }
        if (v instanceof Enum<?> e) {
            return e.getDeclaringClass().getSimpleName() + "." + e.name();
        }
        return v.getClass().getSimpleName();
    }

    /**
     * 本类及其全部祖先的 {@code .json} 逐层深合并（根 → 叶，子类同名叶子键覆盖父类）。
     * 与 {@code PluginExtraProps.load()} 的 {@code visitAncestorsClass(clazz)} 顺序一致，
     * 因此父类 {@code .json} 里写的 {@code dftVal} 在这里同样能被看到。
     */
    private static Map<String, JSONObject> mergedFormResource(Class<?> clazz) {
        List<Class<?>> chain = new ArrayList<>();
        for (Class<?> c = clazz; c != null && !Object.class.equals(c); c = c.getSuperclass()) {
            chain.add(c);
        }
        // 根 → 叶
        chain.sort(Comparator.comparingInt(c -> -depthOf(c)));

        Map<String, JSONObject> merged = new LinkedHashMap<>();
        for (Class<?> c : chain) {
            JSONObject json = TisPluginFormTestSupport.formResource(c.getName());
            if (json == null) {
                continue;
            }
            for (Map.Entry<String, Object> e : json.entrySet()) {
                Object v = e.getValue();
                if (!(v instanceof JSONObject child)) {
                    continue;
                }
                JSONObject base = merged.get(e.getKey());
                if (base == null) {
                    merged.put(e.getKey(), (JSONObject) child.clone());
                } else {
                    base.putAll(child);
                }
            }
        }
        return merged;
    }

    private static int depthOf(Class<?> c) {
        int d = 0;
        for (Class<?> s = c; s != null && !Object.class.equals(s); s = s.getSuperclass()) {
            d++;
        }
        return d;
    }

    /**
     * workshop 包下的全部「具体类 + 其祖先链」。起点是 sezpoz 索引（新增插件自动进入覆盖范围），
     * 再沿 {@code getSuperclass()} 补齐不带 {@code @TISExtension} 的抽象父类 —— 那些父类正是
     * 承载 {@code dftVal} 的地方，不能漏。
     */
    private static Set<Class<?>> workshopClasses() {
        Set<Class<?>> result = new TreeSet<>(Comparator.comparing(Class::getName));
        for (Class<?> seed : TisPluginFormTestSupport.packageClasses(PKG_ROOT)) {
            for (Class<?> c = seed; c != null && !Object.class.equals(c); c = c.getSuperclass()) {
                if (!c.getName().startsWith(PKG_ROOT + ".")) {
                    break;
                }
                result.add(c);
            }
        }
        Assert.assertFalse("扫描不到任何 workshop 类 —— sezpoz 索引或包名前缀写错了", result.isEmpty());
        return result;
    }
}
