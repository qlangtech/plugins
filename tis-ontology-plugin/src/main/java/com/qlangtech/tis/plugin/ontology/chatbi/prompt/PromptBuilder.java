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

import java.util.List;

/**
 * Prompt 拼装器（§4.2 T2）。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/6/2
 */
public class PromptBuilder {

    public static List<String> buildSystemPrompt() {
        return List.of(PromptTemplate.SYSTEM_PROMPT);
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
     */
    public static String extractSqlFromCodeBlock(String llmResponse) {
        if (llmResponse == null || llmResponse.isEmpty()) {
            return "";
        }

        String trimmed = llmResponse.trim();

        // 匹配 ```sql ... ``` 或 ``` ... ```
        if (trimmed.startsWith("```")) {
            int firstNewline = trimmed.indexOf('\n');
            if (firstNewline == -1) {
                return "";
            }
            int endIdx = trimmed.lastIndexOf("```");
            if (endIdx <= firstNewline) {
                return "";
            }
            return trimmed.substring(firstNewline + 1, endIdx).trim();
        }

        return trimmed;
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
