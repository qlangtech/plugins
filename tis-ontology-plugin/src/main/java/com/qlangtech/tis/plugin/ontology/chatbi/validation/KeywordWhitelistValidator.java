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
import org.apache.commons.lang3.StringUtils;

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

    // 默认配置（用于向后兼容）
    private static final Set<String> DEFAULT_ALLOWED_FIRST_KEYWORDS = Set.of(
            "SELECT", "WITH", "EXPLAIN", "SHOW", "DESC", "DESCRIBE"
    );

    private static final Set<String> DEFAULT_FORBIDDEN_KEYWORDS = Set.of(
            "DROP", "DELETE", "TRUNCATE", "ALTER", "INSERT", "UPDATE",
            "GRANT", "REVOKE", "EXEC", "EXECUTE", "CREATE", "REPLACE"
    );

    private static final Set<String> DEFAULT_SAFE_FUNCTIONS = Set.of(
            "REPLACE", "TRIM", "SUBSTRING", "CONCAT", "CAST", "CONVERT"
    );

    @Override
    public ValidationResult validate(String sql, RetrievalResult context) {
        return validate(sql, context, null);
    }

    /**
     * 使用配置进行校验
     */
    public ValidationResult validate(String sql, RetrievalResult context,
                                    com.qlangtech.tis.plugin.ontology.chatbi.config.ValidationConfig config) {
        if (sql == null || sql.isBlank()) {
            return ValidationResult.fail("SQL is empty");
        }

        // 从配置或使用默认值
        Set<String> allowedFirstKeywords = config != null ? config.getAllowedFirstKeywordsSet() : DEFAULT_ALLOWED_FIRST_KEYWORDS;
        Set<String> forbiddenKeywords = config != null ? config.getForbiddenKeywordsSet() : DEFAULT_FORBIDDEN_KEYWORDS;
        Set<String> safeFunctions = config != null ? config.getSafeFunctionsSet() : DEFAULT_SAFE_FUNCTIONS;

        String normalized = StringUtils.upperCase(StringUtils.trim(removeComments(sql)));

        // 检查第一个关键字
        String firstKeyword = extractFirstKeyword(normalized);
        if (firstKeyword == null || !allowedFirstKeywords.contains(firstKeyword)) {
            return ValidationResult.fail("First keyword must be one of: " + allowedFirstKeywords + ", but got: " + firstKeyword);
        }

        // 移除安全的函数调用（如 REPLACE(...) 等），避免误判
        String sqlWithoutSafeFunctions = removeSafeFunctionCalls(normalized, safeFunctions);

        // 检查危险关键字（按 token 边界）
        List<String> forbiddenMatches = new ArrayList<>();
        Pattern keywordPattern = Pattern.compile(
                "\\b(" + String.join("|", forbiddenKeywords) + ")\\b",
                Pattern.CASE_INSENSITIVE
        );
        Matcher matcher = keywordPattern.matcher(sqlWithoutSafeFunctions);
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

    /**
     * 移除安全的函数调用，避免将函数名误判为危险关键字。
     * 例如：REPLACE(...) → ___SAFE_FUNC___
     */
    private String removeSafeFunctionCalls(final String originSql, Set<String> safeFunctions) {
        String sql = originSql;
        for (String func : safeFunctions) {
            // 匹配函数调用模式：FUNC_NAME(...)，支持嵌套括号
            // 使用简单的正则替换，将整个函数调用替换为占位符
            Pattern funcPattern = Pattern.compile(
                    "\\b" + func + "\\s*\\(",
                    Pattern.CASE_INSENSITIVE
            );

            Matcher matcher = funcPattern.matcher(sql);
            StringBuilder result = new StringBuilder();
            int lastEnd = 0;

            while (matcher.find()) {
                try {
                    result.append(sql, lastEnd, matcher.start());
                } catch (Exception e) {
                    throw new RuntimeException(result.toString() + "\n-----------------------------\n" + sql + "\n-----------------------------\n" + lastEnd + "\n-----------------------------\n" + matcher.start());
                }
                // 找到函数调用的起始位置，现在需要找到匹配的右括号
                int parenCount = 1;
                int pos = matcher.end();
                while (pos < sql.length() && parenCount > 0) {
                    char c = sql.charAt(pos);
                    if (c == '(') parenCount++;
                    else if (c == ')') parenCount--;
                    pos++;
                }
                // 替换整个函数调用为占位符
                result.append("___SAFE_FUNC___");
                lastEnd = pos;
            }
            result.append(sql.substring(lastEnd));
            sql = result.toString();
        }
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
