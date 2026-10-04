package com.qlangtech.tis.plugin.ontology.workshop;

import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.extension.Descriptor;
import org.junit.Assert;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * 「聚合属性插件族」结构回归测试的公共反射工具。
 *
 * <p>从 {@code model.config.TestWorkshopConfigPlugins} 里抽出来：那套「扫 sezpoz 索引取子类 →
 * 反射实例化嵌套 descriptor → 读 classpath 上的 .json」的手法，在每新增一族多态字段时都要
 * 重复一遍（{@code model.header} 是第二处），复制粘帖会让两边悄悄漂移。
 *
 * <p>全部方法<b>不依赖 TIS 运行期</b>（不需要 {@code --add-opens}，也不需要在
 * {@code @Before} 里 Assume 跳过）：类清单来自编译期生成的 sezpoz 注解索引，资源靠
 * classloader 读，descriptor 直接 {@code newInstance}。这样调用方在默认 surefire 配置下
 * 就能真跑，不会因为环境问题变成「一直绿但什么都没查」。
 */
public final class TisPluginFormTestSupport {

    /**
     * sezpoz 在编译期为 {@code @TISExtension} 生成的扩展点索引 —— TIS 的 ExtensionFinder
     * 实际读的就是它（{@code @TISExtension} 是 {@code RetentionPolicy.CLASS}，
     * 运行期 {@code getAnnotation} 恒为 null，不能直接查注解）。
     */
    public static final String SEZPOZ_INDEX_PATH =
            "/META-INF/annotations/com.qlangtech.tis.extension.TISExtension.txt";

    private TisPluginFormTestSupport() {
    }

    /**
     * 索引里 {@code pkgPrefix} 包下的具体子类 —— 与 {@code base} 可赋值者。
     *
     * <p>从索引枚举而非硬编码类名清单：新增子类只要带 {@code @TISExtension}
     * 就自动进入覆盖范围，不会因为测试忘了同步而漏检。
     *
     * <p>扫不到任何子类时<b>直接断言失败</b>而不是返回空集：返回空集会让调用方的
     * 「双向相等」断言在另一侧也为空时假通过（例如注解处理器没生效时）。
     */
    public static Set<Class<?>> concreteSubclassesOf(Class<?> base, String pkgPrefix) {
        Set<Class<?>> result = new TreeSet<>(Comparator.comparing(Class::getName));
        for (Class<?> clazz : packageClasses(pkgPrefix)) {
            if (Modifier.isAbstract(clazz.getModifiers()) || base.equals(clazz)) {
                continue;
            }
            if (base.isAssignableFrom(clazz)) {
                result.add(clazz);
            }
        }
        Assert.assertFalse(base.getSimpleName() + " 在 sezpoz 索引里一个具体子类都没扫到"
                + "（注解处理器未生效？扫描包 " + pkgPrefix + " 写错了？）", result.isEmpty());
        return result;
    }

    /** 与 {@code None} 前缀约定对应的关闭子类 */
    public static Class<?> offSubclassOf(Class<?> base, String pkgPrefix) {
        for (Class<?> sub : concreteSubclassesOf(base, pkgPrefix)) {
            if (sub.getSimpleName().startsWith("None")) {
                return sub;
            }
        }
        throw new AssertionError(base.getSimpleName() + " 下没有以 None 开头的关闭子类");
    }

    /** sezpoz 索引中 {@code pkgPrefix} 包下的类（已加载，按名字排序，保证失败信息稳定） */
    public static List<Class<?>> packageClasses(String pkgPrefix) {
        List<Class<?>> result = new ArrayList<>();
        for (String fqcn : sezpozIndexEntries()) {
            if (!fqcn.startsWith(pkgPrefix)) {
                continue;
            }
            Class<?> clazz = outerClassOf(fqcn);
            if (clazz != null && result.stream().noneMatch(c -> c == clazz)) {
                result.add(clazz);
            }
        }
        Assert.assertFalse("sezpoz 索引中 " + pkgPrefix + " 下没有任何条目", result.isEmpty());
        return result;
    }

    /** 类的嵌套 descriptor（约定名 {@code DefaultDescriptor} 优先，否则取第一个） */
    public static Descriptor<?> descriptorOf(Class<?> pluginClass) {
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

    /**
     * 表单资源：{@code /<FQCN 的点号换成斜杠>.json}；不存在返回 null。
     *
     * <p>返回 null 而非断言失败是有意的 —— 调用方分两种场景：<em>必须</em>有 json 的
     * （带 {@code @FormField} 字段的子类）自行断言非空；<em>必须</em>没有 json 的
     * （零字段的关态子类）则断言为空。
     */
    public static JSONObject formResource(String fqcn) {
        String path = "/" + fqcn.replace('.', '/') + ".json";
        try (InputStream in = TisPluginFormTestSupport.class.getResourceAsStream(path)) {
            if (in == null) {
                return null;
            }
            return JSONObject.parseObject(new String(in.readAllBytes(), StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new AssertionError("读取表单资源失败：" + path, e);
        }
    }

    public static Set<String> names(Set<Class<?>> classes) {
        return classes.stream().map(Class::getSimpleName).collect(Collectors.toCollection(TreeSet::new));
    }

    /** 索引条目所在的外部类；已是顶层类则返回自身，加载不到返回 null */
    private static Class<?> outerClassOf(String fqcn) {
        try {
            Class<?> clazz = Class.forName(fqcn, false, TisPluginFormTestSupport.class.getClassLoader());
            Class<?> outer = clazz.getEnclosingClass();
            return outer != null ? outer : clazz;
        } catch (Throwable t) {
            return null;
        }
    }

    private static Descriptor<?> newDescriptor(Class<?> descriptorClass) {
        try {
            return (Descriptor<?>) descriptorClass.getDeclaredConstructor().newInstance();
        } catch (Throwable t) {
            return null;
        }
    }

    private static List<String> sezpozIndexEntries() {
        try (InputStream in = TisPluginFormTestSupport.class.getResourceAsStream(SEZPOZ_INDEX_PATH)) {
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
