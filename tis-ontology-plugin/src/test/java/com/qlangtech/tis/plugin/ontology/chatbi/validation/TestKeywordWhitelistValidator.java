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
package com.qlangtech.tis.plugin.ontology.chatbi.validation;

import com.qlangtech.tis.plugin.ontology.graphrag.RetrievalResult;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

/**
 * KeywordWhitelistValidator 测试用例（§5.1 第 0 步）。
 * <p>
 * 验收标准：
 * - 拦截危险关键字（DROP/DELETE/TRUNCATE 等）
 * - 豁免列名中的匹配（如 drop_count、delete_flag）
 * - 允许安全的查询（SELECT/WITH/EXPLAIN）
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/6/2
 */
public class TestKeywordWhitelistValidator {

    private final KeywordWhitelistValidator validator = new KeywordWhitelistValidator();
    private final RetrievalResult mockContext = new RetrievalResult("", List.of(), List.of(), List.of());

    @Test
    public void testValidSelect() {
        String sql = "SELECT * FROM users WHERE status = 'active'";
        ValidationResult result = validator.validate(sql, mockContext);
        assertTrue("Valid SELECT should pass", result.valid());
    }

    @Test
    public void testValidWith() {
        String sql = "WITH tmp AS (SELECT id FROM orders) SELECT * FROM tmp";
        ValidationResult result = validator.validate(sql, mockContext);
        assertTrue("Valid WITH should pass", result.valid());
    }

    @Test
    public void testValidExplain() {
        String sql = "EXPLAIN SELECT COUNT(*) FROM users";
        ValidationResult result = validator.validate(sql, mockContext);
        assertTrue("Valid EXPLAIN should pass", result.valid());
    }

    @Test
    public void testForbiddenDrop() {
        String sql = "DROP TABLE users";
        ValidationResult result = validator.validate(sql, mockContext);
        assertFalse("DROP TABLE should be blocked", result.valid());
        assertTrue("Error should mention DROP", result.reason().contains("DROP"));
    }

    @Test
    public void testForbiddenDelete() {
        String sql = "DELETE FROM users WHERE id = 1";
        ValidationResult result = validator.validate(sql, mockContext);
        assertFalse("DELETE should be blocked", result.valid());
        assertTrue("Error should mention DELETE", result.reason().contains("DELETE"));
    }

    @Test
    public void testForbiddenTruncate() {
        String sql = "TRUNCATE TABLE orders";
        ValidationResult result = validator.validate(sql, mockContext);
        assertFalse("TRUNCATE should be blocked", result.valid());
        assertTrue("Error should mention TRUNCATE", result.reason().contains("TRUNCATE"));
    }

    @Test
    public void testForbiddenInsert() {
        String sql = "INSERT INTO users (name) VALUES ('hacker')";
        ValidationResult result = validator.validate(sql, mockContext);
        assertFalse("INSERT should be blocked", result.valid());
        assertTrue("Error should mention INSERT", result.reason().contains("INSERT"));
    }

    @Test
    public void testForbiddenUpdate() {
        String sql = "UPDATE users SET role = 'admin' WHERE id = 1";
        ValidationResult result = validator.validate(sql, mockContext);
        assertFalse("UPDATE should be blocked", result.valid());
        assertTrue("Error should mention UPDATE", result.reason().contains("UPDATE"));
    }

    /**
     * 关键测试：列名中包含危险关键字应被豁免（不误拦）。
     */
    @Test
    public void testColumnNameWithDropShouldPass() {
        String sql = "SELECT drop_count, drop_rate FROM metrics WHERE drop_count > 10";
        ValidationResult result = validator.validate(sql, mockContext);
        assertTrue("Column name 'drop_count' should NOT be blocked", result.valid());
    }

    @Test
    public void testColumnNameWithDeleteShouldPass() {
        String sql = "SELECT user_id, delete_flag FROM orders WHERE delete_flag = 0";
        ValidationResult result = validator.validate(sql, mockContext);
        assertTrue("Column name 'delete_flag' should NOT be blocked", result.valid());
    }

    @Test
    public void testColumnNameWithTruncateShouldPass() {
        String sql = "SELECT truncate_time FROM logs ORDER BY truncate_time DESC";
        ValidationResult result = validator.validate(sql, mockContext);
        assertTrue("Column name 'truncate_time' should NOT be blocked", result.valid());
    }

    /**
     * 边界测试：注释中的危险关键字应被忽略。
     */
    @Test
    public void testCommentShouldBeIgnored() {
        String sql = "-- This query will NOT drop the table\nSELECT * FROM users";
        ValidationResult result = validator.validate(sql, mockContext);
        assertTrue("Comment with DROP should be ignored", result.valid());
    }

    @Test
    public void testMultiLineCommentShouldBeIgnored() {
        String sql = "/* DELETE operation is disabled */\nSELECT id FROM orders";
        ValidationResult result = validator.validate(sql, mockContext);
        assertTrue("Multi-line comment with DELETE should be ignored", result.valid());
    }

    /**
     * 恶意测试：尝试绕过（应被拦截）。
     */
    @Test
    public void testCaseInsensitiveDrop() {
        String sql = "DrOp TaBlE users";
        ValidationResult result = validator.validate(sql, mockContext);
        assertFalse("Case-insensitive DROP should be blocked", result.valid());
    }

    @Test
    public void testSelectThenDrop() {
        String sql = "SELECT * FROM users; DROP TABLE users;";
        ValidationResult result = validator.validate(sql, mockContext);
        assertFalse("SQL injection with DROP should be blocked", result.valid());
    }

    @Test
    public void testInvalidFirstKeyword() {
        String sql = "CREATE TABLE users (id INT)";
        ValidationResult result = validator.validate(sql, mockContext);
        assertFalse("CREATE as first keyword should be blocked", result.valid());
        assertTrue("Error should mention first keyword", result.reason().contains("First keyword"));
    }

    @Test
    public void testEmptySql() {
        String sql = "";
        ValidationResult result = validator.validate(sql, mockContext);
        assertFalse("Empty SQL should fail", result.valid());
    }

    @Test
    public void testNullSql() {
        String sql = null;
        ValidationResult result = validator.validate(sql, mockContext);
        assertFalse("Null SQL should fail", result.valid());
    }
}
