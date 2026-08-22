package com.qlangtech.tis.plugin.ontology.chatbi;

import com.alibaba.fastjson.JSON;
import com.qlangtech.tis.manage.common.TisUTF8;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Falcon ChatBI 测试数据集加载器
 * 用于加载和查询 Falcon dev.json 测试数据
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/06/02
 */
public class FalconChatBITestExample {

    public static final String FALCON_DIR_ROOT = "/opt/misc/Falcon";

    private static final String DEFAULT_DEV_JSON_PATH = FALCON_DIR_ROOT + "/dev_data/dev.json";

    private final List<FalconTestCase> allTestCases;
    private final Map<String, List<FalconTestCase>> testCasesByDbId;

    private FalconChatBITestExample(List<FalconTestCase> testCases) {
        this.allTestCases = Collections.unmodifiableList(testCases);
        this.testCasesByDbId = testCases.stream()
                .collect(Collectors.groupingBy(
                        FalconTestCase::getDbId,
                        LinkedHashMap::new,
                        Collectors.toList()
                ));
    }

    /**
     * 从默认路径加载测试数据集
     */
    public static FalconChatBITestExample load() throws IOException {
        return load(DEFAULT_DEV_JSON_PATH);
    }

    /**
     * 从指定路径加载测试数据集
     */
    public static FalconChatBITestExample load(String jsonFilePath) throws IOException {
        String jsonContent = new String(Files.readAllBytes(Paths.get(jsonFilePath)));
        List<FalconTestCase> testCases = JSON.parseArray(jsonContent, FalconTestCase.class);
        return new FalconChatBITestExample(testCases);
    }

    public List<FalconTestCase> getTestCasesByDbId(String dbId) {
        return getTestCasesByDbId(dbId, Optional.empty(), false);
    }

    /**
     * 根据 db_id 获取所有测试案例
     */
    public List<FalconTestCase> getTestCasesByDbId(String dbId, Optional<Integer> questionIdCriteria, boolean collectAfterQuestionIdCriteria) {
        List<FalconTestCase> testCases = testCasesByDbId.getOrDefault(dbId, Collections.emptyList());
        return questionIdCriteria.map((questionId) -> {
            AtomicBoolean collect = new AtomicBoolean(false);
            return testCases.stream().filter((cas) -> {
                if (collect.get()) {
                    return true;
                }
                if (questionId.equals(cas.getQuestionId())) {
                    if (collectAfterQuestionIdCriteria) {
                        collect.set(true);
                    }
                    return true;
                }
                return false;
            }).toList();
        }).orElse(testCases);
    }

    /**
     * 获取所有的 db_id 列表
     */
    public Set<String> getAllDbIds() {
        return testCasesByDbId.keySet();
    }

    /**
     * 获取所有测试案例
     */
    public List<FalconTestCase> getAllTestCases() {
        return allTestCases;
    }

    /**
     * 获取指定 db_id 的测试案例数量
     */
    public int getTestCaseCountByDbId(String dbId) {
        return getTestCasesByDbId(dbId).size();
    }

    /**
     * 获取总测试案例数量
     */
    public int getTotalTestCaseCount() {
        return allTestCases.size();
    }

    /**
     * 获取 db_id 的数量
     */
    public int getDbIdCount() {
        return testCasesByDbId.size();
    }

    /**
     * 获取每个 db_id 对应的测试案例数量统计
     */
    public Map<String, Integer> getTestCaseCountsByDbId() {
        return testCasesByDbId.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue().size(),
                        (a, b) -> a,
                        LinkedHashMap::new
                ));
    }

    /**
     * 根据 question_id 查找测试案例
     */
    public Optional<FalconTestCase> getTestCaseByQuestionId(int questionId) {
        return allTestCases.stream()
                .filter(tc -> tc.getQuestionId() == questionId)
                .findFirst();
    }

    /**
     * 打印统计信息
     */
    public void printStatistics() {
        System.out.println("=== Falcon 测试数据集统计 ===");
        System.out.println("总测试案例数: " + getTotalTestCaseCount());
        System.out.println("数据库数量: " + getDbIdCount());
        System.out.println("\n各数据库测试案例数量:");

        getTestCaseCountsByDbId().forEach((dbId, count) ->
                System.out.println("  db_id=" + dbId + ": " + count + " 个测试案例")
        );
    }

    /**
     * 打印指定 db_id 的所有测试案例
     */
    public void printTestCasesByDbId(String dbId) {
        List<FalconTestCase> testCases = getTestCasesByDbId(dbId);
        System.out.println("\n=== db_id=" + dbId + " 的测试案例 (共 " + testCases.size() + " 个) ===");

        for (FalconTestCase testCase : testCases) {
            System.out.println("\n[Question " + testCase.getQuestionId() + "]");
            System.out.println("问题: " + testCase.getQuestion());
            System.out.println("SQL: " + testCase.getFirstSql());
            System.out.println("是否顺序敏感: " + testCase.isOrderSensitive());
        }
    }

    /**
     * 示例用法
     */
    public static void main(String[] args) throws IOException {
        FalconChatBITestExample example = FalconChatBITestExample.load();

        // 打印统计信息
        example.printStatistics();


        example.getTestCaseCountsByDbId().forEach((dbId, count) -> {
                    File examList = new File(FalconChatBITestExample.FALCON_DIR_ROOT, "test_exam/" + dbId + "/test_exam.md");

                    try {
                        FileUtils.touch(examList);
                        System.out.println("  db_id=" + dbId + ": " + count + " 个测试案例");
                        List<FalconTestCase> testCases = example.getTestCasesByDbId(dbId);
                        int ii = 0;
                        for (FalconTestCase c : testCases) {
                            System.out.println(++ii + ":" + c.getQuestion());
                        }
                        int index = 0;
                        try (PrintStream writer = new PrintStream(examList, TisUTF8.get())) {
                            writer.println("<!--write by: /Users/mozhenghua/j2ee_solution/project/plugins/tis-ontology-plugin/src/test/java/com/qlangtech/tis/plugin/ontology/chatbi/FalconChatBITestExample.java-->");
                            writer.println();
                            for (FalconTestCase c : testCases) {
                                writer.println("## " + (++index) + ". Q：" + c.getQuestion());
                                writer.println();
                                writer.println("### QuestionId:" + c.getQuestionId());
                                writer.println();
                                writer.println("### A.SQL");
                                writer.println();
                                writer.println("```sql");
                                try (BufferedReader reader = new BufferedReader(new StringReader(String.join(";\n", c.getSql())))) {
                                    String line = null;
                                    while (StringUtils.isNotEmpty(line = reader.readLine())) {
                                        writer.println(line);
                                    }
                                }
                                writer.println();
                                writer.println("```");
                                writer.println();

                                writer.println("### A.Result");
                                writer.println();
                                List<Map<String, List<String>>> answers = c.getAnswer();
                                if (answers == null || answers.isEmpty()) {
                                    writer.println("*(无结果数据)*");
                                } else {
                                    for (Map<String, List<String>> answerGroup : answers) {
                                        if (answerGroup == null || answerGroup.isEmpty()) {
                                            continue;
                                        }

                                        // 收集所有列名并保持顺序
                                        Set<String> columns = new LinkedHashSet<>(answerGroup.keySet());

                                        // 确定行数（取所有列值列表的最大长度）
                                        int rowCount = 0;
                                        for (List<String> values : answerGroup.values()) {
                                            if (values != null) {
                                                rowCount = Math.max(rowCount, values.size());
                                            }
                                        }

                                        if (rowCount == 0) {
                                            continue;
                                        }

                                        // 打印表头
                                        writer.print("| ");
                                        for (String col : columns) {
                                            writer.print(col.replace("|", "\\|") + " | ");
                                        }
                                        writer.println();

                                        // 打印分隔符
                                        writer.print("|");
                                        for (String col : columns) {
                                            writer.print(" --- |");
                                        }
                                        writer.println();

                                        // 按行打印数据
                                        for (int i = 0; i < rowCount; i++) {
                                            writer.print("| ");
                                            for (String col : columns) {
                                                List<String> values = answerGroup.get(col);
                                                String cell = "";
                                                if (values != null && i < values.size()) {
                                                    cell = values.get(i);
                                                    if (cell == null) {
                                                        cell = "";
                                                    }
                                                }
                                                // 对 markdown 特殊字符做简单转义
                                                writer.print(cell.replace("|", "\\|").replace("\n", " ") + " | ");
                                            }
                                            writer.println();
                                        }
                                        writer.println();
                                    }
                                }
                                writer.println();
                            }


                        }
                        System.out.println();
                    } catch (IOException e) {
                        throw new RuntimeException("file:" + examList.getAbsolutePath(), e);
                    }
                }
        );


//        // 获取某个 db_id 的测试案例
//        String testDbId = "14";
//        List<FalconTestCase> testCases = example.getTestCasesByDbId(testDbId);
//        System.out.println("\n获取 db_id=" + testDbId + " 的测试案例数量: " + testCases.size());
//
//        // 打印某个 db_id 的所有测试案例
//        if (!testCases.isEmpty()) {
//            example.printTestCasesByDbId(testDbId);
//        }
//
//        // 根据 question_id 查找
//        Optional<FalconTestCase> testCase = example.getTestCaseByQuestionId(0);
//        testCase.ifPresent(tc -> {
//            System.out.println("\n=== 查找 question_id=0 的测试案例 ===");
//            System.out.println(tc);
//        });
    }
}
