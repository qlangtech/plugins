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
import org.apache.commons.lang3.StringUtils;

import java.util.List;

import static com.qlangtech.tis.plugin.ontology.graphrag.SubgraphSnapshot.ObjectTypeNode.KEY_PHYSICAL_EXPRESSION;

/**
 * Prompt 拼装器（§4.2 T2）。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/6/2
 */
public class PromptBuilder {

    /**
     * 构建 System Prompt（条件性包含物理表达式规则）
     *
     * @param graphragContext GraphRAG 检索到的业务上下文
     */
    public static List<String> buildSystemPrompt(String graphragContext) {
        boolean hasPhysicalExpr = StringUtils.contains(graphragContext,"**" + KEY_PHYSICAL_EXPRESSION + "=");// graphragContext != null && graphragContext.contains("**" + KEY_PHYSICAL_EXPRESSION + "=");
        return List.of(PromptTemplate.buildSystemPrompt(hasPhysicalExpr));
    }

    public static UserPrompt buildInitialPrompt(String nlq, String graphragContext) {
        String userPromptText = PromptTemplate.buildUserPrompt(graphragContext, nlq);
        return new UserPrompt("ChatBI: " + nlq, userPromptText);
    }

    public static UserPrompt buildRetryPrompt(String nlq, String graphragContext, String previousSql, String errorMessage) {
        String userPromptText = PromptTemplate.buildRetryPrompt(graphragContext, nlq, previousSql, errorMessage);
        return new UserPrompt("ChatBI-Retry: " + nlq, userPromptText);
    }

    /**
     * 从 LLM 返回的 markdown 代码块中提取 SQL。
     * 支持格式：```sql\nSELECT...\n``` 或 ```\nSELECT...\n```
     * 也支持前面有分析文本的情况（例如：字段映射分析 + 代码块）
     */
    public static String extractSqlFromCodeBlock(String llmResponse) {
        if (llmResponse == null || llmResponse.isEmpty()) {
            return "";
        }

        String content = llmResponse.trim();

        // 查找代码块起始标记（```sql 或 ```）
        int codeBlockStart = content.indexOf("```sql");
        boolean hasSqlMarker = true;
        if (codeBlockStart == -1) {
            // 尝试查找通用的 ```
            codeBlockStart = content.indexOf("```");
            hasSqlMarker = false;
        }

        if (codeBlockStart == -1) {
            // 没有代码块标记，返回原文本
            return content;
        }

        // 找到第一个换行符（代码块内容的起始）
        int contentStart = content.indexOf('\n', codeBlockStart);
        if (contentStart == -1) {
            return "";
        }
        contentStart++; // 跳过换行符

        // 找到代码块结束标记
        int codeBlockEnd = content.indexOf("```", contentStart);
        if (codeBlockEnd == -1) {
            // 没有结束标记，取到末尾
            return content.substring(contentStart).trim();
        }

        // 提取代码块内容
        return content.substring(contentStart, codeBlockEnd).trim();
    }

    /**
     * 估算 prompt token 数（粗略估计：中文 2 字符 = 1 token，英文 4 字符 = 1 token）。
     */
    public static int estimateTokens(String text) {
        if (text == null) return 0;
        int chineseCount = 0;
        int otherCount = 0;
        for (char c : text.toCharArray()) {
            if (c >= 0x4E00 && c <= 0x9FA5) {
                chineseCount++;
            } else {
                otherCount++;
            }
        }
        return chineseCount / 2 + otherCount / 4;
    }
}
