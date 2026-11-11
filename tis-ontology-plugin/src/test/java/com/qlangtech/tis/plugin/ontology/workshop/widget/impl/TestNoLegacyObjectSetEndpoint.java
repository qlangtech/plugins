package com.qlangtech.tis.plugin.ontology.workshop.widget.impl;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * 已废弃的「通用对象集查询 REST 链路」不得复活。
 *
 * <h3>被钉住的四样东西</h3>
 * <p>2026-11 之前，对象集数据的唯一来源是前端 {@code VariableService.computeObjectSetDefinition}
 * → {@code OntologyService.queryObjectSet()} → {@code WorkshopOntologyAction#doQueryObjectSet}
 * → {@code WorkshopOntologyService#queryObjectSet()}。它有两条硬伤：
 * <ol>
 *   <li>行数由前端常量 {@code OBJECT_SET_QUERY_LIMIT} 决定，配置里没有对应字段；</li>
 *   <li>排序在浏览器内存里做，只排「已经取回的那一批」。</li>
 * </ol>
 * <p>现在取数<b>全部收归各 widget 自己的 descriptor {@code httpProcess("get_data")}</b>，
 * 排序与条数在 SQL 里一次完成。这条迁移横跨三个仓库（{@code tis-console} 前端 +
 * 其 Java 侧、{@code plugins/tis-ontology-plugin}），任何一处回退都<b>不会编译失败</b>：
 * 旧的端点若被重新引入，前端哪都能跑，只是又回到了「拉一份明细进内存」的形态。
 *
 * <h3>为什么是 grep 型测试</h3>
 * <p>删掉的类已经不存在，没有类型可以让人「顺手 import 回来」时被编译器拦住；
 * 能拦住的范围只剩源码文本。扫描范围刻意含<b>注释</b>：留着
 * 「本组件通过 queryObjectSet 取数」这类注释，下一个维护者会照它把链路接回来。
 *
 * <p>兄弟仓缺席时 {@link Assume} 跳过（与 {@code TestWidgetFieldStructure} 同一口径）。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/11/11
 */
public class TestNoLegacyObjectSetEndpoint {

    /** 兄弟仓相对本模块根目录的候选位置（surefire 的 cwd 是模块 basedir） */
    private static final String[] CONSOLE_DIR_CANDIDATES =
            {"../../../tis-console", "../../tis-console", "../tis-console"};

    /** 要扫的子目录：前端 TS 与它的 Java 侧 */
    private static final String[] SCAN_SUBDIRS = {"src/workshop", "src/main/java"};

    /**
     * 旧链路的四个标识符。
     *
     * <p>{@code do_query_object_set} / {@code workshop_ontology_action} 是前端拼的
     * {@code event_submit_do_xxx} 与 {@code action} 参数（下划线形态在 TS 里以字符串出现）；
     * 另外两个是类名。
     */
    private static final String[] FORBIDDEN = {
            "queryObjectSet",
            "WorkshopOntologyService",
            "WorkshopOntologyAction",
            "doQueryObjectSet",
            "do_query_object_set",
            "workshop_ontology_action",
    };

    @Test
    public void noLegacyReferencesRemain() throws IOException {
        Path consoleRoot = locateConsoleRoot();
        Assume.assumeTrue("未找到前端工程（候选见 stdout），跳过旧链路残留检查", consoleRoot != null);

        List<String> hits = new ArrayList<>();
        int scanned = 0;
        for (String subdir : SCAN_SUBDIRS) {
            Path dir = consoleRoot.resolve(subdir);
            if (!Files.isDirectory(dir)) {
                continue;
            }
            try (Stream<Path> files = Files.walk(dir)) {
                List<Path> targets = files.filter(Files::isRegularFile)
                        .filter(p -> isScannable(p.getFileName().toString()))
                        .filter(p -> {
                            String n = p.toAbsolutePath().normalize().toString();
                            return !n.contains("/node_modules/") && !n.contains("/target/");
                        })
                        .toList();
                scanned += targets.size();
                targets.forEach(p -> collectHits(consoleRoot, p, hits));
            }
        }

        // 护栏：扫描范围一旦失效（目录改名、路径候选全错），hits 恒为空，本测试会静默变绿
        Assert.assertTrue("只扫到 " + scanned + " 个文件，扫描范围失效（目录改名了？）", scanned > 50);
        Assert.assertTrue("旧的对象集查询链路又被引用了（取数应走各 widget 的 get_data）：\n  "
                + String.join("\n  ", hits), hits.isEmpty());
    }

    /** 只看源码：{@code .ts/.html/.java} */
    private static boolean isScannable(String fileName) {
        return fileName.endsWith(".ts") && !fileName.endsWith(".spec.ts")
                || fileName.endsWith(".html")
                || fileName.endsWith(".java");
    }

    private static void collectHits(Path root, Path file, List<String> hits) {
        List<String> lines;
        try {
            lines = Files.readAllLines(file, StandardCharsets.UTF_8);
        } catch (IOException e) {
            hits.add(root.relativize(file) + " 读取失败：" + e.getMessage());
            return;
        }
        for (int i = 0; i < lines.size(); i++) {
            String line = lines.get(i);
            for (String token : FORBIDDEN) {
                if (line.contains(token)) {
                    hits.add(root.relativize(file) + ":" + (i + 1) + " 命中 `" + token + "`");
                    break;
                }
            }
        }
    }

    private static Path locateConsoleRoot() {
        String override = System.getProperty("tis.console.dir");
        List<Path> roots = new ArrayList<>();
        if (override != null && !override.trim().isEmpty()) {
            roots.add(Paths.get(override.trim()));
        }
        for (String candidate : CONSOLE_DIR_CANDIDATES) {
            roots.add(Paths.get(candidate));
        }
        for (Path root : roots) {
            if (Files.isDirectory(root.resolve("src/workshop"))) {
                return root;
            }
        }
        System.out.println("[TestNoLegacyObjectSetEndpoint] 未找到前端工程，已尝试："
                + roots.stream().map(p -> p.toAbsolutePath().normalize().toString()).toList());
        return null;
    }
}
