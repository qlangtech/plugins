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

package com.qlangtech.tis.plugin.ontology.impl.action.type;

import com.qlangtech.tis.plugin.ds.IDBReservedKeys;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.CreateLinkRule;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.DeleteLinkRule;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.OntologyActionRule;
import com.qlangtech.tis.plugin.ontology.impl.action.type.LinkActionType.LinkDefinition;
import com.qlangtech.tis.plugin.ontology.impl.action.type.LinkActionType.TableRef;
import com.qlangtech.tis.plugin.ontology.impl.linker.Cardinality;
import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Objects;

/**
 * 把 {@link CreateLinkRule} / {@link DeleteLinkRule} 翻译成对对象表 / 连接表的 DML。
 *
 * <p>TIS 没有链接实例存储，{@link LinkDefinition} 描述链接的物理形态：
 * <ul>
 *   <li>{@code ObjectTypeForeignKeys}：源表上的外键列，创建 = UPDATE，删除 = 置 NULL；</li>
 *   <li>{@code JoinTableDataset} / {@code BackingObjectType}：连接表里的一行，创建 = INSERT，删除 = DELETE。</li>
 * </ul>
 *
 * <p>三张表可能绑定不同的数据源，因此每条 SQL 都通过对应 {@link TableRef#dataSource()} 取连接；
 * 跨数据源时无法保证原子性（先读源/目标值，再写），这一点与 Ontology 的其余跨库能力一致。
 *
 * <p>幂等口径：
 * <ul>
 *   <li>FK 已指向同一目标 / 连接行已存在 → 直接成功；</li>
 *   <li>FK 已指向另一个目标 → 报错「先删除现有链接」，不静默覆盖；</li>
 *   <li>删除未命中 → 报「link not found」（与 {@code deleteObject} 的 0 行口径一致）。</li>
 * </ul>
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/20
 */
final class LinkActionExecutor {

    private LinkActionExecutor() {
    }

    static void execute(LinkDefinition definition, OntologyActionRule rule, Map<String, Object> values) {
        Objects.requireNonNull(definition, "link definition can not be null");
        String sourceParam = LinkActionType.sourceRidParamName(rule);
        String targetParam = LinkActionType.targetRidParamName(rule);
        Object sourceRid = requireParam(values, sourceParam, "源对象");
        Object targetRid = requireParam(values, targetParam, "目标对象");

        if (rule instanceof CreateLinkRule createRule) {
            rejectBidirectional(createRule.bidirectional);
            switch (definition.relationshipType()) {
                case ObjectTypeForeignKeys -> createForeignKeyLink(definition, sourceRid, targetRid);
                case JoinTableDataset, BackingObjectType -> createJoinLink(definition, sourceRid, targetRid);
            }
            return;
        }
        if (rule instanceof DeleteLinkRule deleteRule) {
            rejectBidirectional(deleteRule.deleteBidirectional);
            switch (definition.relationshipType()) {
                case ObjectTypeForeignKeys -> deleteForeignKeyLink(definition, sourceRid, targetRid);
                case JoinTableDataset, BackingObjectType -> deleteJoinLink(definition, sourceRid, targetRid);
            }
            return;
        }
        throw new IllegalStateException("unsupported link rule: "
                + (rule == null ? "null" : rule.getClass().getName()));
    }

    /**
     * TIS 没有链接实例存储，也就不存在「反向链接」；静默忽略会让用户以为双向已生效。
     */
    static void rejectBidirectional(Boolean bidirectional) {
        if (Boolean.TRUE.equals(bidirectional)) {
            throw new UnsupportedOperationException(
                    "bidirectional links are not supported: 链接实例没有独立存储，无法创建反向链接");
        }
    }

    private static Object requireParam(Map<String, Object> values, String name, String end) {
        Object value = values.get(name);
        if (isBlankValue(value)) {
            throw new IllegalStateException(end + " RID 参数 '" + name + "' 为空：请在运行时表单中填写或选择");
        }
        return value;
    }

    // ------------------------------------------------------------------
    // ObjectTypeForeignKeys
    // ------------------------------------------------------------------

    private static void createForeignKeyLink(LinkDefinition def, Object sourceRid, Object targetRid) {
        QueryResult target = queryColumnValue(def.target(), def.targetField(), targetRid);
        if (!target.found()) {
            throw new IllegalStateException("目标对象不存在：" + def.target().objectType()
                    + " " + def.target().pk() + "=" + targetRid);
        }
        if (isBlankValue(target.value())) {
            throw new IllegalStateException("目标对象的连接字段为空：" + def.target().objectType()
                    + "." + def.targetField());
        }

        // 外键列同时是源对象主键（1:1 依赖表）：链接值就是源主键，无法再指向别的目标
        if (def.source().pk().equals(def.sourceField())) {
            if (sameValue(sourceRid, target.value())) {
                return;
            }
            throw new IllegalStateException("源对象的外键列 '" + def.sourceField()
                    + "' 同时是主键，无法建立指向该目标的链接");
        }

        QueryResult source = queryColumnValue(def.source(), def.sourceField(), sourceRid);
        if (!source.found()) {
            throw new IllegalStateException("源对象不存在：" + def.source().objectType()
                    + " " + def.source().pk() + "=" + sourceRid);
        }
        if (!isBlankValue(source.value())) {
            if (sameValue(source.value(), target.value())) {
                // 已指向同一目标，幂等成功
                return;
            }
            throw new IllegalStateException("源对象已链接到其它目标（" + def.sourceField()
                    + "=" + source.value() + "），请先删除现有链接");
        }
        if (def.cardinality() == Cardinality.ONE_ONE) {
            ensureOneToOne(def, target.value(), sourceRid);
        }
        updateLinkColumn(def.source(), def.sourceField(), target.value(), sourceRid);
    }

    private static void deleteForeignKeyLink(LinkDefinition def, Object sourceRid, Object targetRid) {
        QueryResult source = queryColumnValue(def.source(), def.sourceField(), sourceRid);
        if (!source.found()) {
            throw new IllegalStateException("源对象不存在：" + def.source().objectType()
                    + " " + def.source().pk() + "=" + sourceRid);
        }
        QueryResult target = queryColumnValue(def.target(), def.targetField(), targetRid);
        if (!target.found()) {
            throw new IllegalStateException("目标对象不存在：" + def.target().objectType()
                    + " " + def.target().pk() + "=" + targetRid);
        }
        if (isBlankValue(source.value()) || !sameValue(source.value(), target.value())) {
            throw new IllegalStateException("link not found: " + def.describe());
        }
        if (def.source().pk().equals(def.sourceField())) {
            throw new IllegalStateException("源对象的外键列 '" + def.sourceField()
                    + "' 同时是主键，不能置空；请改为删除该对象");
        }
        updateLinkColumn(def.source(), def.sourceField(), null, sourceRid);
    }

    /**
     * 一对一约束：同一个目标值只能被一个源对象引用。
     */
    private static void ensureOneToOne(LinkDefinition def, Object targetValue, Object sourceRid) {
        TableRef source = def.source();
        IDBReservedKeys ds = source.dataSource();
        String sql = "SELECT COUNT(*) FROM " + ds.getEscapedEntity(source.physicalTable())
                + " WHERE " + ds.getEscapedEntity(def.sourceField()) + "=?"
                + " AND " + ds.getEscapedEntity(source.pk()) + "<>?";
        long occupied = countRows(source, sql, targetValue, sourceRid);
        if (occupied > 0) {
            throw new IllegalStateException("一对一链接冲突：已有其它源对象指向目标值 '"
                    + targetValue + "'（" + def.describe() + "）");
        }
    }

    // ------------------------------------------------------------------
    // JoinTableDataset / BackingObjectType
    // ------------------------------------------------------------------

    private static void createJoinLink(LinkDefinition def, Object sourceRid, Object targetRid) {
        Object sourceValue = requireLinkValue(def.source(), def.sourceField(), sourceRid, "源对象");
        Object targetValue = requireLinkValue(def.target(), def.targetField(), targetRid, "目标对象");
        TableRef join = def.join();
        if (existsJoinRow(join, def.joinSourceField(), sourceValue, def.joinTargetField(), targetValue)) {
            // 连接行已存在，幂等成功
            return;
        }
        insertJoinRow(join, def.joinSourceField(), sourceValue, def.joinTargetField(), targetValue);
    }

    private static void deleteJoinLink(LinkDefinition def, Object sourceRid, Object targetRid) {
        Object sourceValue = requireLinkValue(def.source(), def.sourceField(), sourceRid, "源对象");
        Object targetValue = requireLinkValue(def.target(), def.targetField(), targetRid, "目标对象");
        TableRef join = def.join();
        IDBReservedKeys ds = join.dataSource();
        String sql = "DELETE FROM " + ds.getEscapedEntity(join.physicalTable())
                + " WHERE " + ds.getEscapedEntity(def.joinSourceField()) + "=?"
                + " AND " + ds.getEscapedEntity(def.joinTargetField()) + "=?";

        int[] affected = {0};
        join.dataSource().visitFirstConnection((conn) -> {
            try (PreparedStatement statement = conn.preparedStatement(sql)) {
                statement.setObject(1, sourceValue);
                statement.setObject(2, targetValue);
                affected[0] = statement.executeUpdate();
            } catch (SQLException e) {
                throw new RuntimeException("failed to delete link, SQL:" + sql, e);
            }
        });
        if (affected[0] == 0) {
            throw new IllegalStateException("link not found: " + def.describe());
        }
    }

    /**
     * 按主键定位对象行并取出连接列的值；行不存在 / 连接值为空都报可读错误，
     * 避免把 null 写进连接表。
     */
    private static Object requireLinkValue(TableRef table, String linkField, Object keyVal, String end) {
        QueryResult result = queryColumnValue(table, linkField, keyVal);
        if (!result.found()) {
            throw new IllegalStateException(end + "不存在：" + table.objectType()
                    + " " + table.pk() + "=" + keyVal);
        }
        if (isBlankValue(result.value())) {
            throw new IllegalStateException(end + "的连接字段为空：" + table.objectType() + "." + linkField);
        }
        return result.value();
    }

    private static boolean existsJoinRow(TableRef join, String col1, Object v1, String col2, Object v2) {
        IDBReservedKeys ds = join.dataSource();
        String sql = "SELECT COUNT(*) FROM " + ds.getEscapedEntity(join.physicalTable())
                + " WHERE " + ds.getEscapedEntity(col1) + "=?"
                + " AND " + ds.getEscapedEntity(col2) + "=?";
        return countRows(join, sql, v1, v2) > 0;
    }

    private static void insertJoinRow(TableRef join, String col1, Object v1, String col2, Object v2) {
        IDBReservedKeys ds = join.dataSource();
        String sql = "INSERT INTO " + ds.getEscapedEntity(join.physicalTable())
                + " (" + ds.getEscapedEntity(col1) + "," + ds.getEscapedEntity(col2) + ") VALUES (?,?)";
        join.dataSource().visitFirstConnection((conn) -> {
            try (PreparedStatement statement = conn.preparedStatement(sql)) {
                statement.setObject(1, v1);
                statement.setObject(2, v2);
                statement.executeUpdate();
            } catch (SQLException e) {
                throw new RuntimeException("failed to insert link, SQL:" + sql, e);
            }
        });
    }

    // ------------------------------------------------------------------
    // 通用 SQL 封装
    // ------------------------------------------------------------------

    private record QueryResult(boolean found, Object value) {
    }

    /**
     * {@code SELECT selectCol FROM table WHERE pk=?}；{@code found=false} 表示行不存在。
     */
    private static QueryResult queryColumnValue(TableRef table, String selectCol, Object keyVal) {
        IDBReservedKeys ds = table.dataSource();
        String sql = "SELECT " + ds.getEscapedEntity(selectCol)
                + " FROM " + ds.getEscapedEntity(table.physicalTable())
                + " WHERE " + ds.getEscapedEntity(table.pk()) + "=?";

        QueryResult[] result = {new QueryResult(false, null)};
        table.dataSource().visitFirstConnection((conn) -> {
            try (PreparedStatement statement = conn.preparedStatement(sql)) {
                statement.setObject(1, keyVal);
                try (ResultSet rs = statement.executeQuery()) {
                    if (rs.next()) {
                        result[0] = new QueryResult(true, rs.getObject(1));
                    }
                }
            } catch (SQLException e) {
                throw new RuntimeException("failed to query link column, SQL:" + sql, e);
            }
        });
        return result[0];
    }

    /**
     * {@code UPDATE table SET col=? WHERE pk=?}。
     *
     * <p>调用前都已确认目标值与当前值不同（创建时源值为空、删除时源值等于目标值），
     * 因此 0 行只可能是「源对象不存在」。
     */
    private static void updateLinkColumn(TableRef table, String column, Object value, Object keyVal) {
        IDBReservedKeys ds = table.dataSource();
        String sql = "UPDATE " + ds.getEscapedEntity(table.physicalTable())
                + " SET " + ds.getEscapedEntity(column) + "=?"
                + " WHERE " + ds.getEscapedEntity(table.pk()) + "=?";
        int[] affected = {0};
        table.dataSource().visitFirstConnection((conn) -> {
            try (PreparedStatement statement = conn.preparedStatement(sql)) {
                statement.setObject(1, value);
                statement.setObject(2, keyVal);
                affected[0] = statement.executeUpdate();
            } catch (SQLException e) {
                throw new RuntimeException("failed to update link column, SQL:" + sql, e);
            }
        });
        if (affected[0] == 0) {
            throw new IllegalStateException("源对象不存在：" + table.objectType()
                    + " " + table.pk() + "=" + keyVal);
        }
    }

    private static long countRows(TableRef table, String sql, Object v1, Object v2) {
        long[] count = {0};
        table.dataSource().visitFirstConnection((conn) -> {
            try (PreparedStatement statement = conn.preparedStatement(sql)) {
                statement.setObject(1, v1);
                statement.setObject(2, v2);
                try (ResultSet rs = statement.executeQuery()) {
                    if (rs.next()) {
                        count[0] = rs.getLong(1);
                    }
                }
            } catch (SQLException e) {
                throw new RuntimeException("failed to count rows, SQL:" + sql, e);
            }
        });
        return count[0];
    }

    static boolean isBlankValue(Object value) {
        return value == null || (value instanceof CharSequence seq && StringUtils.isBlank(seq));
    }

    /**
     * JDBC 取回的数值类型可能是 {@code Integer} / {@code Long} / {@code BigDecimal}，
     * 与表单里的字符串直接 {@code equals} 会误判，故先按字符串比，再按数值比。
     */
    static boolean sameValue(Object a, Object b) {
        if (Objects.equals(a, b)) {
            return true;
        }
        if (a == null || b == null) {
            return false;
        }
        String sa = String.valueOf(a).trim();
        String sb = String.valueOf(b).trim();
        if (sa.equals(sb)) {
            return true;
        }
        try {
            return new BigDecimal(sa).compareTo(new BigDecimal(sb)) == 0;
        } catch (NumberFormatException e) {
            return false;
        }
    }
}
