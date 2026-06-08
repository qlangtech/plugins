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

import com.qlangtech.tis.plugin.ontology.graphrag.LinkerInfo;
import com.qlangtech.tis.plugin.ontology.graphrag.RetrievalResult;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectVisitorAdapter;
import net.sf.jsqlparser.util.TablesNamesFinder;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * AST 校验器（§5.1 T3）：检查表名/列名/JOIN 是否在 GraphRAG 白名单内。
 * <p>
 * 使用 JSqlParser 解析 SQL 为 AST，支持多种 SQL 方言（MySQL, PostgreSQL, Oracle 等）。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/6/2
 */
public class AstValidator implements SqlValidator {

    @Override
    public ValidationResult validate(String sql, RetrievalResult context) {
        if (StringUtils.isBlank(sql)) {
            return ValidationResult.fail("SQL is empty");
        }

        // 去除末尾的分号
        sql = StringUtils.substringBeforeLast(StringUtils.trim(sql), ";");//.trim();
//        if (sql.endsWith(";")) {
//            sql = sql.substring(0, sql.length() - 1).trim();
//        }

        try {
            // 解析 SQL（支持多种方言）
            Statement statement = CCJSqlParserUtil.parse(sql);

            // 使用 TablesNamesFinder 收集表名
            TablesNamesFinder tablesFinder = new TablesNamesFinder();
            List<String> tableNames = tablesFinder.getTableList(statement);

            // 收集 JOIN 关系和中间表
            JoinCollector joinCollector = new JoinCollector();
            if (statement instanceof Select) {
                Select select = (Select) statement;
                select.getSelectBody().accept(joinCollector);
            }

            // 构建白名单集合
            Set<String> allowedTables = new HashSet<>(context.objectTypes());
            Set<JoinPair> allowedJoins = buildJoinWhitelist(context);

            // 校验表名（中间表不需要在白名单中）
            List<String> invalidTables = new ArrayList<>();
            for (String table : tableNames) {
                if (!allowedTables.contains(table) && !joinCollector.intermediateTables.contains(table)) {
                    invalidTables.add(table);
                }
            }

            // 校验 JOIN（涉及中间表的 JOIN 不需要在白名单中）
            List<String> invalidJoins = new ArrayList<>();
            for (JoinPair join : joinCollector.joins) {
                if (!isJoinAllowed(join, allowedJoins, joinCollector.intermediateTables)) {
                    invalidJoins.add(join.left + " <-> " + join.right);
                }
            }

            // 汇总错误
            List<String> issues = new ArrayList<>();
            if (!invalidTables.isEmpty()) {
                issues.add("Invalid tables: " + invalidTables);
            }
            if (!invalidJoins.isEmpty()) {
                issues.add("Invalid joins: " + invalidJoins);
            }

            if (!issues.isEmpty()) {
                return ValidationResult.fail("AST validation failed", issues);
            }

            return ValidationResult.ok();

        } catch (Exception e) {
            return ValidationResult.fail("SQL parsing failed: " + e.getMessage(), new Exception(sql, e));
        }
    }

    private Set<JoinPair> buildJoinWhitelist(RetrievalResult context) {
        Set<JoinPair> pairs = new HashSet<>();
        for (LinkerInfo linker : context.linkers()) {
            pairs.add(new JoinPair(linker.source(), linker.target()));
            pairs.add(new JoinPair(linker.target(), linker.source())); // 双向
        }
        return pairs;
    }

    private boolean isJoinAllowed(JoinPair join, Set<JoinPair> allowedJoins, Set<String> intermediateTables) {
        // 如果 JOIN 涉及中间表（CTE/子查询），则跳过白名单检查
        if (intermediateTables.contains(join.left) || intermediateTables.contains(join.right)) {
            return true;
        }
        return allowedJoins.contains(join);
    }

    /**
     * JOIN 收集器：遍历 AST 收集 JOIN 关系和中间表。
     */
    private static class JoinCollector extends SelectVisitorAdapter {
        final Set<JoinPair> joins = new HashSet<>();
        final Set<String> intermediateTables = new HashSet<>();

        @Override
        public void visit(PlainSelect plainSelect) {
            // 收集 WITH 子句中的 CTE 名称
            if (plainSelect.getWithItemsList() != null) {
                plainSelect.getWithItemsList().forEach(withItem -> {
                    if (withItem.getAlias() != null) {
                        intermediateTables.add(withItem.getAlias().getName());
                    }
                });
            }

            // 收集 JOIN 表
            FromItem fromItem = plainSelect.getFromItem();
            List<Join> joinsList = plainSelect.getJoins();

            if (joinsList != null && fromItem != null) {
                // 构建别名到表名的映射
                java.util.Map<String, String> aliasToTable = new java.util.HashMap<>();

                // 处理 FROM 表
                String fromTableName = extractTableName(fromItem);
                if (fromTableName != null) {
                    String fromAlias = fromItem.getAlias() != null ? fromItem.getAlias().getName() : fromTableName;
                    aliasToTable.put(fromAlias, fromTableName);

                    // 如果 FROM 是子查询且有别名，记录为中间表
                    if (!isPhysicalTable(fromItem)) {
                        intermediateTables.add(fromTableName);
                    }
                }

                // 处理所有 JOIN 表
                for (Join join : joinsList) {
                    FromItem rightItem = join.getRightItem();
                    String rightTableName = extractTableName(rightItem);

                    if (rightTableName != null) {
                        String rightAlias = rightItem.getAlias() != null ? rightItem.getAlias().getName() : rightTableName;
                        aliasToTable.put(rightAlias, rightTableName);

                        // 如果 JOIN 右侧是子查询且有别名，记录为中间表
                        if (!isPhysicalTable(rightItem)) {
                            intermediateTables.add(rightTableName);
                        }
                    }
                }

                // 重新遍历 JOIN，根据 ON 条件提取真实的表关系
                for (Join join : joinsList) {
                    Collection<Expression> onExpressions = join.getOnExpressions();
                    if (onExpressions != null && !onExpressions.isEmpty()) {
                        for (Expression onExpr : onExpressions) {
                            // 从 ON 条件中提取涉及的表
                            Set<String> tablesInCondition = extractTablesFromExpression(onExpr, aliasToTable);

                            // 如果 ON 条件涉及两个表，记录它们的连接关系
                            if (tablesInCondition.size() == 2) {
                                java.util.Iterator<String> iter = tablesInCondition.iterator();
                                String table1 = iter.next();
                                String table2 = iter.next();
                                joins.add(new JoinPair(table1, table2));
                            }
                        }
                    }
                }
            }
        }

        private String extractTableName(FromItem fromItem) {
            if (fromItem instanceof Table) {
                return ((Table) fromItem).getName();
            }
            // 对于子查询，返回其别名
            if (fromItem.getAlias() != null) {
                return fromItem.getAlias().getName();
            }
            return null;
        }

        private boolean isPhysicalTable(FromItem fromItem) {
            return fromItem instanceof Table;
        }

        /**
         * 从表达式中提取涉及的表名（通过列引用的表别名）
         */
        private Set<String> extractTablesFromExpression(Expression expr, java.util.Map<String, String> aliasToTable) {
            Set<String> tables = new HashSet<>();
            String exprStr = expr.toString();

            // 简单解析：提取形如 "alias.column" 的模式
            // 例如：s.Product_ID = p.Product_ID 会提取 s 和 p
            for (String alias : aliasToTable.keySet()) {
                if (exprStr.contains(alias + ".")) {
                    String tableName = aliasToTable.get(alias);
                    if (tableName != null) {
                        tables.add(tableName);
                    }
                }
            }

            return tables;
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
