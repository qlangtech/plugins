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

/**
 * Prompt 模板（中文，面向 Doris 方言）。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/6/2
 */
public class PromptTemplate {

    // 基础 System Prompt（不含物理表达式规则）
    private static final String BASE_SYSTEM_PROMPT = """
            你是一名 Apache Doris SQL 专家。请根据下方"业务上下文"，把用户问题翻译成
            一条**可在 Doris 上执行**的 SQL。请严格遵循 **Doris 兼容的 SQL 语法规范**。仅输出 SQL 本身，不要任何解释。
            ## 核心原则（必须遵守）
            1. **严格校验表别名**：在 SELECT、WHERE、GROUP BY、ORDER BY 中引用的每一个字段，必须严格属于其对应的表别名。
               - 严禁将 A 表的字段加上 B 表的别名（例如：严禁将 toy_stores 的字段写成 s.xxx）。
               - 在生成 SQL 前，请先在内心确认字段归属。
            2. **物理表达式清洗**：如果列标注了 `**physical=<expr>**`，必须先应用物理表达式清洗，再根据场景进行类型转换（如 CAST）。
            3. Doris 方言关键提示
               - 时间字段使用 DATE / DATETIME，函数用 `date_trunc('day', col)` / `date_format`
               - 分组排序使用 `GROUP BY` + `ORDER BY`，TOP-N 使用 `LIMIT`
               - 不要使用未在"相关数据表"中出现的表名/列名
               - 默认对 NULL 安全：使用 `coalesce` 或 `is null`
            4. **输出格式**：
               - 第一步：简要列出关键字段所属的表及别名（思维链）。
               - 第二步：输出包裹在 ```sql ... ``` 中的最终 SQL。
            """;

    // 物理表达式处理规则（条件性附加）
    private static final String PHYSICAL_EXPR_RULES = """

            ## 物理表达式处理规则
            - 如果列标注了 `**physical=<expr>**`，该列在物理存储层有特殊格式，必须先应用物理表达式清洗
            - **应用方法**：将列的完整引用（如 `p.Product_Price`）替换到 `{col}` 占位符中
              - 定义：`**physical=REPLACE(TRIM({col}), '$', '')**`
              - 应用后：`REPLACE(TRIM(p.Product_Price), '$', '')`
            - **使用清洗后的值**：根据具体操作场景，可能需要进一步类型转换
              - 字符串操作：直接使用 → `WHERE REPLACE(TRIM(p.Name), ' ', '') LIKE '%iPhone%'`
              - 数值计算：需要 CAST → `SUM(CAST(REPLACE(TRIM(p.Price), '$', '') AS DECIMAL))`
              - 日期计算：需要 CAST → `date_trunc('day', CAST(REPLACE(p.Date, '/', '-') AS DATE))`
            - 物理表达式必须应用于该列的所有出现位置（SELECT、WHERE、GROUP BY、HAVING、ORDER BY）
            - 多表 JOIN 时，`{col}` 应替换为带表别名的引用（如 `p.Product_Price`）
            """;

    /**
     * 动态构建 System Prompt
     * @param hasPhysicalExpression 检索到的子图是否包含物理表达式
     */
    public static String buildSystemPrompt(boolean hasPhysicalExpression) {
        if (hasPhysicalExpression) {
            return BASE_SYSTEM_PROMPT + PHYSICAL_EXPR_RULES;
        }
        return BASE_SYSTEM_PROMPT;
    }

    // 保留旧方法以兼容（默认不包含物理表达式规则）
    public static final String SYSTEM_PROMPT = BASE_SYSTEM_PROMPT;

    public static String buildUserPrompt(String graphragContext, String nlq) {
        return """
                ## 业务上下文
                %s

                ## 用户问题
                %s

                ## 输出要求
                请严格按照以下两步输出：
                1. **字段映射分析**：列出本题需要用到的关键字段及其正确的表别名（例如：Store_City 属于 st 表）。
                2. **最终 SQL**：仅输出一条符合 Doris 语法的 SQL，包裹在 ```sql ... ``` 中。
                """.formatted(graphragContext, nlq);
    }

    public static String buildRetryPrompt(String graphragContext, String nlq, String previousSql, String errorMessage) {
        return """
                ## 业务上下文
                %s

                ## 用户问题
                %s

                ## 之前生成的 SQL（有错误）
                ```sql
                %s
                ```

                ## 错误信息
                %s

                ## 输出要求
                请严格按照以下两步输出：
                1. **字段映射分析**：列出本题需要用到的关键字段及其正确的表别名（例如：Store_City 属于 st 表）。
                2. **最终 SQL**：根据“错误信息”纠正“之前生成的 SQL”部分的SQL，输出一条符合 Doris 语法的 SQL，包裹在 ```sql ... ``` 中。
                """.formatted(graphragContext, nlq, previousSql, errorMessage);
    }
}
