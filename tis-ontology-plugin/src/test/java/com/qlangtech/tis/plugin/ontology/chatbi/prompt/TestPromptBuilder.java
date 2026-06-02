/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.qlangtech.tis.plugin.ontology.chatbi.prompt;

import com.qlangtech.tis.aiagent.llm.UserPrompt;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * PromptBuilder 测试用例（§4.2 T2）。
 * <p>
 * 验收标准：
 * - 正确提取 SQL from markdown 代码块
 * - 豁免非代码块的 SQL
 * - 估算 token 数（中英文混合）
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/6/2
 */
public class TestPromptBuilder {

    @Test
    public void testExtractSqlFromCodeBlockWithSqlTag() {
        String llmResponse = "```sql\nSELECT * FROM users\n```";
        String sql = PromptBuilder.extractSqlFromCodeBlock(llmResponse);
        assertEquals("SELECT * FROM users", sql);
    }

    @Test
    public void testExtractSqlFromCodeBlockWithoutSqlTag() {
        String llmResponse = "```\nSELECT COUNT(*) FROM orders\n```";
        String sql = PromptBuilder.extractSqlFromCodeBlock(llmResponse);
        assertEquals("SELECT COUNT(*) FROM orders", sql);
    }

    @Test
    public void testExtractSqlWithMultiLineQuery() {
        String llmResponse = """
                ```sql
                SELECT user_id, COUNT(*) as order_count
                FROM orders
                WHERE status = 'completed'
                GROUP BY user_id
                ```
                """;
        String sql = PromptBuilder.extractSqlFromCodeBlock(llmResponse);
        assertTrue("Should contain SELECT", sql.contains("SELECT"));
        assertTrue("Should contain GROUP BY", sql.contains("GROUP BY"));
    }

    @Test
    public void testExtractSqlFromPlainText() {
        String llmResponse = "SELECT * FROM users";
        String sql = PromptBuilder.extractSqlFromCodeBlock(llmResponse);
        assertEquals("SELECT * FROM users", sql);
    }

    @Test
    public void testExtractSqlWithExtraText() {
        String llmResponse = "Here's the SQL:\n```sql\nSELECT id FROM orders\n```\nThis query returns order IDs.";
        String sql = PromptBuilder.extractSqlFromCodeBlock(llmResponse);
        assertEquals("SELECT id FROM orders", sql);
    }

    @Test
    public void testExtractSqlEmpty() {
        String llmResponse = "";
        String sql = PromptBuilder.extractSqlFromCodeBlock(llmResponse);
        assertEquals("", sql);
    }

    @Test
    public void testExtractSqlNull() {
        String llmResponse = null;
        String sql = PromptBuilder.extractSqlFromCodeBlock(llmResponse);
        assertEquals("", sql);
    }

    @Test
    public void testExtractSqlInvalidCodeBlock() {
        String llmResponse = "```sql\nSELECT * FROM"; // 缺少结束标记
        String sql = PromptBuilder.extractSqlFromCodeBlock(llmResponse);
        assertEquals("", sql);
    }

    @Test
    public void testBuildInitialPrompt() {
        String nlq = "统计每个城市的订单数量";
        String graphragContext = "## 相关数据表\n- orders(city, status)\n";
        UserPrompt prompt = PromptBuilder.buildInitialPrompt(nlq, graphragContext);

        assertNotNull(prompt);
        assertEquals("ChatBI: 统计每个城市的订单数量", prompt.getAbstractInfo());
        assertTrue("Should contain NLQ", prompt.getPrompt().contains(nlq));
        assertTrue("Should contain GraphRAG context", prompt.getPrompt().contains(graphragContext));
    }

    @Test
    public void testBuildRetryPrompt() {
        String nlq = "查询活跃用户数";
        String graphragContext = "## 相关数据表\n- users(id, status)\n";
        String previousSql = "SELECT * FROM user"; // 错误的表名
        String errorMessage = "Table 'user' not found";

        UserPrompt prompt = PromptBuilder.buildRetryPrompt(nlq, graphragContext, previousSql, errorMessage);

        assertNotNull(prompt);
        assertTrue("Should contain previous SQL", prompt.getPrompt().contains(previousSql));
        assertTrue("Should contain error message", prompt.getPrompt().contains(errorMessage));
        assertTrue("Should contain NLQ", prompt.getPrompt().contains(nlq));
    }

    @Test
    public void testEstimateTokensChinese() {
        String text = "统计每个城市的订单数量"; // 10 个汉字
        int tokens = PromptBuilder.estimateTokens(text);
        assertEquals("10 汉字 ≈ 5 tokens", 5, tokens);
    }

    @Test
    public void testEstimateTokensEnglish() {
        String text = "SELECT COUNT(*) FROM orders"; // 28 个字符
        int tokens = PromptBuilder.estimateTokens(text);
        assertEquals("28 英文字符 ≈ 7 tokens", 7, tokens);
    }

    @Test
    public void testEstimateTokensMixed() {
        String text = "查询用户的 order_count"; // 5 汉字 + 13 英文
        int tokens = PromptBuilder.estimateTokens(text);
        // 5 汉字 = 2.5 tokens, 13 英文 = 3.25 tokens, 总共 ≈ 5-6 tokens
        assertTrue("Mixed text should estimate correctly", tokens >= 5 && tokens <= 6);
    }

    @Test
    public void testEstimateTokensEmpty() {
        String text = "";
        int tokens = PromptBuilder.estimateTokens(text);
        assertEquals(0, tokens);
    }

    @Test
    public void testEstimateTokensNull() {
        String text = null;
        int tokens = PromptBuilder.estimateTokens(text);
        assertEquals(0, tokens);
    }

    @Test
    public void testBuildSystemPrompt() {
        var systemPrompt = PromptBuilder.buildSystemPrompt();
        assertNotNull(systemPrompt);
        assertEquals("Should have one system prompt", 1, systemPrompt.size());
        assertTrue("Should mention Doris", systemPrompt.get(0).contains("Doris"));
        assertTrue("Should mention SQL", systemPrompt.get(0).contains("SQL"));
    }
}
