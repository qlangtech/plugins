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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 关键字硬白名单校验器（§5.1 第 0 步）。
 * <p>
 * 拦截危险关键字（DROP/DELETE/TRUNCATE 等），但豁免列名中的匹配（如 drop_count）。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/6/2
 */
public class KeywordWhitelistValidator implements SqlValidator {

    private static final Set<String> ALLOWED_FIRST_KEYWORDS = Set.of(
            "SELECT", "WITH", "EXPLAIN", "SHOW", "DESC", "DESCRIBE"
    );

    private static final Set<String> FORBIDDEN_KEYWORDS = Set.of(
            "DROP", "DELETE", "TRUNCATE", "ALTER", "INSERT", "UPDATE",
            "GRANT", "REVOKE", "EXEC", "EXECUTE", "CREATE", "REPLACE"
    );

    // 匹配 SQL 关键字（按 token 边界），忽略标识符内的匹配
    private static final Pattern KEYWORD_PATTERN = Pattern.compile(
            "\\b(" + String.join("|", FORBIDDEN_KEYWORDS) + ")\\b",
            Pattern.CASE_INSENSITIVE
    );

    @Override
    public ValidationResult validate(String sql, RetrievalResult context) {
        if (sql == null || sql.isBlank()) {
            return ValidationResult.fail("SQL is empty");
        }

        String normalized = removeComments(sql).trim().toUpperCase();

        // 检查第一个关键字
        String firstKeyword = extractFirstKeyword(normalized);
        if (firstKeyword == null || !ALLOWED_FIRST_KEYWORDS.contains(firstKeyword)) {
            return ValidationResult.fail("First keyword must be one of: " + ALLOWED_FIRST_KEYWORDS + ", but got: " + firstKeyword);
        }

        // 检查危险关键字（按 token 边界）
        List<String> forbiddenMatches = new ArrayList<>();
        Matcher matcher = KEYWORD_PATTERN.matcher(normalized);
        while (matcher.find()) {
            forbiddenMatches.add(matcher.group(1));
        }

        if (!forbiddenMatches.isEmpty()) {
            return ValidationResult.fail(
                    "Forbidden keywords detected: " + forbiddenMatches,
                    forbiddenMatches
            );
        }

        return ValidationResult.ok();
    }

    private String removeComments(String sql) {
        // 移除单行注释 --
        sql = sql.replaceAll("--[^\n]*", "");
        // 移除多行注释 /* */
        sql = sql.replaceAll("/\\*.*?\\*/", " ");
        return sql;
    }

    private String extractFirstKeyword(String sql) {
        String[] tokens = sql.split("\\s+");
        if (tokens.length == 0) {
            return null;
        }
        return tokens[0];
    }
}
