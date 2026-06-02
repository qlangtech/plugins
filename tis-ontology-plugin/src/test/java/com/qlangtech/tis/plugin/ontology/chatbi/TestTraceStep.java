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
package com.qlangtech.tis.plugin.ontology.chatbi;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

/**
 * TraceStep 测试用例（§7 T6）。
 * <p>
 * 验收标准：
 * - 正确构造各类 trace 步骤
 * - JSON 序列化包含必要字段
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/6/2
 */
public class TestTraceStep {

    @Test
    public void testRetrieveStep() {
        TraceStep step = TraceStep.retrieve(3, 2, 150);
        assertEquals("retrieve", step.step());
        assertTrue("Should be ok", step.ok());
        assertEquals("Should have 3 OTs", 3, step.data().getIntValue("otsCount"));
        assertEquals("Should have 2 linkers", 2, step.data().getIntValue("linkersCount"));
        assertEquals(150L, step.millis());
    }

    @Test
    public void testPromptStep() {
        TraceStep step = TraceStep.prompt(1842, "system prompt", "user prompt");
        assertEquals("prompt", step.step());
        assertTrue(step.ok());
        assertEquals(1842, step.data().getIntValue("tokens"));
        assertEquals("system prompt", step.data().getString("system"));
        assertEquals("user prompt", step.data().getString("user"));
    }

    @Test
    public void testLlmStep() {
        TraceStep step = TraceStep.llm("qwen-max", 1842, 124, "```sql\nSELECT...\n```", 1420);
        assertEquals("llm", step.step());
        assertTrue(step.ok());
        assertEquals("qwen-max", step.data().getString("model"));
        assertEquals(1842L, step.data().getLongValue("promptTokens"));
        assertEquals(124L, step.data().getLongValue("completionTokens"));
        assertTrue(step.data().getString("raw").contains("SELECT"));
        assertEquals(1420L, step.millis());
    }

    @Test
    public void testExtractStep() {
        TraceStep step = TraceStep.extract("SELECT * FROM users");
        assertEquals("extract", step.step());
        assertTrue(step.ok());
        assertEquals("SELECT * FROM users", step.data().getString("sql"));
    }

    @Test
    public void testValidateStepSuccess() {
        TraceStep step = TraceStep.validate(true, "Validation passed", null);
        assertEquals("validate", step.step());
        assertTrue(step.ok());
        assertEquals("Validation passed", step.message());
    }

    @Test
    public void testValidateStepFailure() {
        com.alibaba.fastjson.JSONObject issues = new com.alibaba.fastjson.JSONObject();
        issues.put("issues", List.of("Invalid table: products"));
        TraceStep step = TraceStep.validate(false, "AST validation failed", issues);
        assertEquals("validate", step.step());
        assertFalse("Should not be ok", step.ok());
        assertTrue(step.message().contains("failed"));
        assertNotNull(step.data().get("issues"));
    }

    @Test
    public void testExecuteStep() {
        TraceStep step = TraceStep.execute(12, 230);
        assertEquals("execute", step.step());
        assertTrue(step.ok());
        assertEquals(12, step.data().getIntValue("rows"));
        assertEquals(230L, step.millis());
    }

    @Test
    public void testErrorStep() {
        TraceStep step = TraceStep.error("llm", "Connection timeout");
        assertEquals("llm", step.step());
        assertFalse("Should not be ok", step.ok());
        assertEquals("Connection timeout", step.message());
    }

    @Test
    public void testOfFactory() {
        com.alibaba.fastjson.JSONObject data = new com.alibaba.fastjson.JSONObject();
        data.put("test", "value");
        TraceStep step = TraceStep.of("custom", true, "Custom step", data, 100);
        assertEquals("custom", step.step());
        assertTrue(step.ok());
        assertEquals("Custom step", step.message());
        assertEquals("value", step.data().getString("test"));
        assertEquals(100L, step.millis());
    }
}
