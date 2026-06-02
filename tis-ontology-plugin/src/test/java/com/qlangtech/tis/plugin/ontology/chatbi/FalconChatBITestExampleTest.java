package com.qlangtech.tis.plugin.ontology.chatbi;

import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.*;

/**
 * FalconChatBITestExample 单元测试
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/06/02
 */
public class FalconChatBITestExampleTest {

    @Test
    public void testLoadFalconDataset() throws IOException {
        FalconChatBITestExample example = FalconChatBITestExample.load();

        assertNotNull("测试数据集不应为空", example);
        assertTrue("应至少包含一些测试案例", example.getTotalTestCaseCount() > 0);
        assertTrue("应至少包含一些数据库", example.getDbIdCount() > 0);

        System.out.println("成功加载 " + example.getTotalTestCaseCount() + " 个测试案例");
    }

    @Test
    public void testGetTestCasesByDbId() throws IOException {
        FalconChatBITestExample example = FalconChatBITestExample.load();

        // 测试获取特定 db_id 的测试案例
        String testDbId = "14";
        List<FalconTestCase> testCases = example.getTestCasesByDbId(testDbId);

        assertNotNull("测试案例列表不应为空", testCases);
        assertTrue("db_id=14 应该有测试案例", testCases.size() > 0);

        // 验证所有返回的测试案例确实属于该 db_id
        for (FalconTestCase testCase : testCases) {
            assertEquals("所有测试案例应属于 db_id=" + testDbId, testDbId, testCase.getDbId());
        }

        System.out.println("db_id=" + testDbId + " 包含 " + testCases.size() + " 个测试案例");
    }

    @Test
    public void testGetTestCaseByQuestionId() throws IOException {
        FalconChatBITestExample example = FalconChatBITestExample.load();

        // 测试根据 question_id 查找
        Optional<FalconTestCase> testCase = example.getTestCaseByQuestionId(0);

        assertTrue("应该能找到 question_id=0 的测试案例", testCase.isPresent());
        assertEquals("question_id 应该是 0", 0, testCase.get().getQuestionId());

        System.out.println("找到测试案例: " + testCase.get());
    }

    @Test
    public void testTestCaseStructure() throws IOException {
        FalconChatBITestExample example = FalconChatBITestExample.load();

        // 获取第一个测试案例并验证其结构
        List<FalconTestCase> allCases = example.getAllTestCases();
        assertFalse("应该有测试案例", allCases.isEmpty());

        FalconTestCase firstCase = allCases.get(0);
        assertNotNull("问题不应为空", firstCase.getQuestion());
        assertNotNull("SQL 不应为空", firstCase.getSql());
        assertFalse("SQL 列表不应为空", firstCase.getSql().isEmpty());
        assertNotNull("第一条 SQL 不应为空", firstCase.getFirstSql());

        System.out.println("第一个测试案例:");
        System.out.println("  question_id: " + firstCase.getQuestionId());
        System.out.println("  db_id: " + firstCase.getDbId());
        System.out.println("  question: " + firstCase.getQuestion());
        System.out.println("  SQL 数量: " + firstCase.getSql().size());
        System.out.println("  是否顺序敏感: " + firstCase.isOrderSensitive());
    }

    @Test
    public void testStatistics() throws IOException {
        FalconChatBITestExample example = FalconChatBITestExample.load();

        // 打印统计信息
        example.printStatistics();

        // 验证统计信息的一致性
        int totalFromMap = example.getTestCaseCountsByDbId().values().stream()
                .mapToInt(Integer::intValue)
                .sum();

        assertEquals("总数应该一致", example.getTotalTestCaseCount(), totalFromMap);
    }

    @Test
    public void testGetNonExistentDbId() throws IOException {
        FalconChatBITestExample example = FalconChatBITestExample.load();

        // 测试获取不存在的 db_id
        List<FalconTestCase> testCases = example.getTestCasesByDbId("non_existent_db");

        assertNotNull("应该返回空列表而不是 null", testCases);
        assertTrue("不存在的 db_id 应该返回空列表", testCases.isEmpty());
    }

    @Test
    public void testGetNonExistentQuestionId() throws IOException {
        FalconChatBITestExample example = FalconChatBITestExample.load();

        // 测试获取不存在的 question_id
        Optional<FalconTestCase> testCase = example.getTestCaseByQuestionId(-1);

        assertFalse("不存在的 question_id 应该返回空 Optional", testCase.isPresent());
    }
}
