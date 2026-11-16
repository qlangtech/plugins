package com.qlangtech.tis.plugin.ontology.workshop.desc;

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
 * 已删除的独立变量 CRUD 端点 {@code WorkshopVariableOperation} 不得复活。
 *
 * <h3>被钉住的东西</h3>
 * <p>2026-11 之前，变量的 list / get / delete 走独立描述符
 * {@code ...workshop.desc.WorkshopVariableOperation$DefaultDescriptor}
 * （前端以其 FQCN 作 {@code impl}，type 为裸 {@code get | list | delete}）。
 * 现在三个操作收编进 {@code WorkshopModule.DefaultDescriptor.httpProcess}，
 * type 加 {@code variable_} 前缀（variable_get / variable_list / variable_delete），
 * 与模块操作共用同一个 {@code impl}。这条迁移横跨两个仓库
 * （{@code tis-console} 前端 + 本插件），任何一处回退都<b>不会编译失败</b>：
 * 旧类若被重新引入并注册，前端照旧能跑，只是模块与变量的端点又分了家。
 *
 * <h3>为什么是 grep 型测试</h3>
 * <p>删掉的类已经不存在，没有类型可以让人「顺手 import 回来」时被编译器拦住；
 * 能拦住的范围只剩源码文本。扫描范围刻意含<b>注释</b>：留着
 * 「变量 CRUD 走 WorkshopVariableOperation」这类注释，下一个维护者会照它把链路接回来。
 *
 * <p>扫描范围只含<b>前端仓</b>：本测试自身源码里就有这些 token 字符串，
 * 扫 plugins 仓会自命中；Java 侧的复活由
 * {@code TestWorkshopVariableTypes#variableEndpointsAreAddressableByModuleDescriptorAndLegacyImplIsGone}
 * 的反向断言拦住，两层互补。
 *
 * <p>兄弟仓缺席时 {@link Assume} 跳过（与 {@code TestNoLegacyObjectSetEndpoint} 同一口径）。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/11/16
 */
public class TestNoLegacyVariableOperationEndpoint {

    /** 兄弟仓相对本模块根目录的候选位置（surefire 的 cwd 是模块 basedir） */
    private static final String[] CONSOLE_DIR_CANDIDATES =
            {"../../../tis-console", "../../tis-console", "../tis-console"};

    /** 要扫的子目录：前端 TS 与它的 Java 侧 */
    private static final String[] SCAN_SUBDIRS = {"src/workshop", "src/main/java"};

    /**
     * 旧端点的标识符：类名（Java 与 TS 里的 FQCN 字符串都以它出现），
     * 以及前端模型层曾经的 impl 常量名。
     */
    private static final String[] FORBIDDEN = {
            "WorkshopVariableOperation",
            "WORKSHOP_VARIABLE_OPERATION_IMPL",
    };

    @Test
    public void noLegacyReferencesRemain() throws IOException {
        Path consoleRoot = locateConsoleRoot();
        Assume.assumeTrue("未找到前端工程（候选见 stdout），跳过旧端点残留检查", consoleRoot != null);

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
        Assert.assertTrue("旧的变量独立端点又被引用了（变量 CRUD 应走 WorkshopModule 端点的 variable_* 分支）：\n  "
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
        System.out.println("[TestNoLegacyVariableOperationEndpoint] 未找到前端工程，已尝试："
                + roots.stream().map(p -> p.toAbsolutePath().normalize().toString()).toList());
        return null;
    }
}
