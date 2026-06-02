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

import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.*;
import com.qlangtech.tis.plugin.ontology.graphrag.LinkerInfo;
import com.qlangtech.tis.plugin.ontology.graphrag.RetrievalResult;

import java.util.*;

/**
 * AST 校验器（§5.1 T3）：检查表名/列名/JOIN 是否在 GraphRAG 白名单内。
 * <p>
 * 使用 Presto Parser（TIS 已集成）解析 SQL 为 AST，遍历节点校验。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/6/2
 */
public class AstValidator implements SqlValidator {

    private static final SqlParser SQL_PARSER = new SqlParser();

    @Override
    public ValidationResult validate(String sql, RetrievalResult context) {
        if (sql == null || sql.isBlank()) {
            return ValidationResult.fail("SQL is empty");
        }

        // 去除末尾的分号（Presto Parser 不接受分号）
        sql = sql.trim();
        if (sql.endsWith(";")) {
            sql = sql.substring(0, sql.length() - 1).trim();
        }

        try {
            ParsingOptions options = new ParsingOptions(ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE);
            Statement statement = SQL_PARSER.createStatement(sql, options);

            // 构建白名单集合
            Set<String> allowedTables = new HashSet<>(context.objectTypes());
            Map<String, Set<String>> allowedColumns = buildColumnWhitelist(context);
            Set<JoinPair> allowedJoins = buildJoinWhitelist(context);

            // 遍历 AST 收集使用的表/列/JOIN
            AstCollector collector = new AstCollector();
            collector.process(statement, null);

            // 校验表名
            List<String> invalidTables = new ArrayList<>();
            for (String table : collector.tables) {
                if (!allowedTables.contains(table)) {
                    invalidTables.add(table);
                }
            }

            // 校验列名（简化版：暂不处理表别名解析，假设列名格式为 table.column）
            List<String> invalidColumns = new ArrayList<>();
            for (String column : collector.columns) {
                if (!isColumnAllowed(column, allowedColumns)) {
                    invalidColumns.add(column);
                }
            }

            // 校验 JOIN（简化版：暂不精确匹配 JOIN ON 条件）
            List<String> invalidJoins = new ArrayList<>();
            for (JoinPair join : collector.joins) {
                if (!isJoinAllowed(join, allowedJoins)) {
                    invalidJoins.add(join.left + " <-> " + join.right);
                }
            }

            // 汇总错误
            List<String> issues = new ArrayList<>();
            if (!invalidTables.isEmpty()) {
                issues.add("Invalid tables: " + invalidTables);
            }
            if (!invalidColumns.isEmpty()) {
                issues.add("Invalid columns: " + invalidColumns);
            }
            if (!invalidJoins.isEmpty()) {
                issues.add("Invalid joins: " + invalidJoins);
            }

            if (!issues.isEmpty()) {
                return ValidationResult.fail("AST validation failed", issues);
            }

            return ValidationResult.ok();

        } catch (Exception e) {
            return ValidationResult.fail("SQL parsing failed: " + e.getMessage());
        }
    }

    private Map<String, Set<String>> buildColumnWhitelist(RetrievalResult context) {
        // 简化实现：从 GraphRAG 的 promptContext 中无法直接提取列名白名单
        // 实际应从 Neo4j 或 OntologyObjectType.getCols() 获取
        // 暂时返回空，表示不校验列名（由 EXPLAIN 兜底）
        return Collections.emptyMap();
    }

    private Set<JoinPair> buildJoinWhitelist(RetrievalResult context) {
        Set<JoinPair> pairs = new HashSet<>();
        for (LinkerInfo linker : context.linkers()) {
            pairs.add(new JoinPair(linker.source(), linker.target()));
            pairs.add(new JoinPair(linker.target(), linker.source())); // 双向
        }
        return pairs;
    }

    private boolean isColumnAllowed(String column, Map<String, Set<String>> allowedColumns) {
        // 简化：暂不校验列名（实际需要从 ObjectType 加载 Property 列表）
        return true;
    }

    private boolean isJoinAllowed(JoinPair join, Set<JoinPair> allowedJoins) {
        return allowedJoins.contains(join);
    }

    /**
     * AST 收集器：遍历 AST 收集表名、列名、JOIN 关系。
     */
    private static class AstCollector extends DefaultTraversalVisitor<Void, Void> {
        final Set<String> tables = new HashSet<>();
        final Set<String> columns = new HashSet<>();
        final Set<JoinPair> joins = new HashSet<>();
        final Set<String> cteNames = new HashSet<>();

        @Override
        protected Void visitWith(With node, Void context) {
            for (WithQuery query : node.getQueries()) {
                cteNames.add(query.getName().toString());
            }
            return super.visitWith(node, context);
        }

        @Override
        protected Void visitTable(Table node, Void context) {
            String tableName = node.getName().toString();
            if (!cteNames.contains(tableName)) {
                tables.add(tableName);
            }
            return super.visitTable(node, context);
        }

        @Override
        protected Void visitDereferenceExpression(DereferenceExpression node, Void context) {
            // table.column 或 alias.column
            String full = node.toString();
            columns.add(full);
            return super.visitDereferenceExpression(node, context);
        }

        @Override
        protected Void visitJoin(Join node, Void context) {
            // 提取左右表名（简化：假设是 Table 节点）
            String left = extractTableName(node.getLeft());
            String right = extractTableName(node.getRight());
            // 只收集非 CTE 表之间的 JOIN，并且排除 CROSS JOIN（没有 ON 条件）
            if (left != null && right != null && !cteNames.contains(left) && !cteNames.contains(right)) {
                // 如果有 JOIN 条件（不是 CROSS JOIN），才需要校验 linker
                if (node.getCriteria().isPresent()) {
                    joins.add(new JoinPair(left, right));
                }
            }
            return super.visitJoin(node, context);
        }

        private String extractTableName(Relation relation) {
            if (relation instanceof Table) {
                return ((Table) relation).getName().toString();
            }
            if (relation instanceof AliasedRelation) {
                return extractTableName(((AliasedRelation) relation).getRelation());
            }
            return null;
        }
    }

    private record JoinPair(String left, String right) {
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof JoinPair pair)) return false;
            // 双向匹配
            return (left.equals(pair.left) && right.equals(pair.right)) ||
                   (left.equals(pair.right) && right.equals(pair.left));
        }

        @Override
        public int hashCode() {
            // 保证双向一致
            return left.hashCode() + right.hashCode();
        }
    }
}
