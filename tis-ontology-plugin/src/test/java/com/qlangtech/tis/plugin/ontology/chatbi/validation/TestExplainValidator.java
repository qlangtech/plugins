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
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * ExplainValidator 测试用例。
 * <p>
 * 注意：这些测试需要真实的数据库连接，因此默认被 @Ignore 标记。
 * 在有数据库环境时，移除 @Ignore 注解来运行测试。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/6/3
 */
public class TestExplainValidator {

    private final RetrievalResult mockContext = new RetrievalResult("", List.of(), List.of(), List.of());

    @Test
    @Ignore("需要真实的数据库连接")
    public void testValidSql() {
        ExplainValidator validator = new ExplainValidator("falcon_14");
        String sql = "SELECT * FROM toy_sales LIMIT 10";

        ValidationResult result = validator.validate(sql, mockContext);
        assertTrue("Valid SQL should pass EXPLAIN validation", result.valid());
    }

    @Test
    @Ignore("需要真实的数据库连接")
    public void testInvalidFunction() {
        ExplainValidator validator = new ExplainValidator("falcon_14");

        // 使用错误的 DATEDIFF 函数签名（SQL Server 风格）
        String sql = "SELECT DATEDIFF('year', CAST(Store_Open_Date AS DATE), DATE '2025-12-31') FROM toy_stores";

        ValidationResult result = validator.validate(sql, mockContext);
        assertFalse("Invalid function signature should fail EXPLAIN validation", result.valid());
        assertTrue("Error message should mention function signature",
                result.reason().toLowerCase().contains("function"));
    }

    @Test
    @Ignore("需要真实的数据库连接")
    public void testTypeMismatch() {
        ExplainValidator validator = new ExplainValidator("falcon_14");

        // 类型不匹配的 SQL
        String sql = "SELECT * FROM toy_sales WHERE Product_ID = 'not_a_number'";

        ValidationResult result = validator.validate(sql, mockContext);
        // 注意：某些数据库可能会自动进行类型转换，这个测试可能通过
        // 实际效果取决于数据库的类型检查严格程度
    }

    @Test
    @Ignore("需要真实的数据库连接")
    public void testTableNotFound() {
        ExplainValidator validator = new ExplainValidator("falcon_14");

        String sql = "SELECT * FROM non_existent_table";

        ValidationResult result = validator.validate(sql, mockContext);
        assertFalse("Non-existent table should fail EXPLAIN validation", result.valid());
    }

    @Test
    @Ignore("需要真实的数据库连接")
    public void testColumnNotFound() {
        ExplainValidator validator = new ExplainValidator("falcon_14");

        String sql = "SELECT non_existent_column FROM toy_sales";

        ValidationResult result = validator.validate(sql, mockContext);
        assertFalse("Non-existent column should fail EXPLAIN validation", result.valid());
    }

    @Test
    public void testEmptySql() {
        ExplainValidator validator = new ExplainValidator("falcon_14");

        ValidationResult result = validator.validate("", mockContext);
        assertFalse("Empty SQL should fail", result.valid());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullDomain() {
        new ExplainValidator(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEmptyDomain() {
        new ExplainValidator("");
    }
}