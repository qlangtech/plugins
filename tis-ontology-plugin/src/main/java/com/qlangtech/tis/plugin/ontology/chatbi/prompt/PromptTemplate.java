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

    public static final String SYSTEM_PROMPT = """
            你是一名 Apache Doris SQL 专家。请根据下方"业务上下文"，把用户问题翻译成
            一条**可在 Doris 上执行**的 SQL。仅输出 SQL 本身，不要任何解释。

            ## Doris 方言关键提示
            - 时间字段使用 DATE / DATETIME，函数用 `date_trunc('day', col)` / `date_format`
            - 分组排序使用 `GROUP BY` + `ORDER BY`，TOP-N 使用 `LIMIT`
            - 不要使用 SQLite 特有函数（如 julianday）
            - 字符串拼接使用 `concat(a, b)`
            - 不要使用未在"相关数据表"中出现的表名/列名
            - 默认对 NULL 安全：使用 `coalesce` 或 `is null`

            ## 物理表达式处理规则（重要）
            - 如果列标注了 `**physical=<expr>**`，在 SQL 中使用该列时，必须将列的完整引用（如 `p.Product_Price`）替换到 `{col}` 占位符中
            - 示例：列 `Product_Price` 标注 `**physical=`REPLACE(TRIM({col}), '$', '')`**`
              - ❌ 错误写法：`CAST(p.Product_Price AS DECIMAL)`
              - ✅ 正确写法：`CAST(REPLACE(TRIM(p.Product_Price), '$', '') AS DECIMAL)`
            - 物理表达式适用于该列在 SQL 中的所有出现位置（SELECT、WHERE、GROUP BY、HAVING、ORDER BY）
            - 多表 JOIN 时注意表别名：`{col}` 应替换为 `<alias>.<column>`（如 `p.Product_Price`）
            - 物理表达式是数据清洗的必要步骤，不能省略
            """;

    public static String buildUserPrompt(String graphragContext, String nlq) {
        return """
                ## 业务上下文
                %s

                ## 用户问题
                %s

                ## 输出
                只输出 SQL，包裹在 ```sql ... ``` 中。
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

                ## 输出
                请修正上述 SQL 的错误，只输出修正后的 SQL，包裹在 ```sql ... ``` 中。
                """.formatted(graphragContext, nlq, previousSql, errorMessage);
    }
}
