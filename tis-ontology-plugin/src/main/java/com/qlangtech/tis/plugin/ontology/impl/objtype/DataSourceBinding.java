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

package com.qlangtech.tis.plugin.ontology.impl.objtype;

import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.ibatis.RowMap;
import com.qlangtech.tis.manage.common.OptionWithEndType;
import com.qlangtech.tis.plugin.IEndTypeGetter;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ds.ColumnMetaData;
import com.qlangtech.tis.plugin.ds.DataSourceFactory;
import com.qlangtech.tis.plugin.ds.TableNotFoundException;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;
import com.qlangtech.tis.util.IPluginContext;
import org.apache.commons.compress.utils.Lists;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/5/8
 */
public class DataSourceBinding extends ObjectTypeBinding {

    private static final String KEY_DISPLAY_NAME = "DataSource Bound";

    private static final String KEY_DB_NAME = "dbName";
    private static final String KEY_PHYSICAL_TABLE_NAME = "physicalTableName";
    @FormField(ordinal = 0, type = FormFieldType.ENUM, validate = {Validator.require})
    public String dbName;

    /**
     * 物理表名（可空）。空时回落到 ObjectType 的 name；
     * 用于「ObjectType 逻辑名 ≠ Doris 物理表名」的场景，例如本体侧用中文语义名而 Doris 侧用下划线英文表名。
     */
    @FormField(ordinal = 1, type = FormFieldType.ENUM, validate = {Validator.require, Validator.db_col_name})
    public String physicalTableName;

    @Override
    public boolean hasBound() {
        return true;
    }

    @Override
    public DataSourceFactory getDataSource() {
        return DataSourceFactory.load(this.dbName);
    }

    @Override
    public List<RowMap> executeQuery(DataSourceQueryCriteria criteria) {
        List<RowMap> rows = Lists.newArrayList();
        StringBuilder sql = new StringBuilder("select ");
        DataSourceFactory ds = getDataSource();
        sql.append(criteria.getTargetCols().stream().map(ds::getEscapedEntity).collect(Collectors.joining(",")));
        sql.append(" from ").append(escapedTableName(ds));

        // WHERE 必须在 ORDER BY / LIMIT 之前：过滤 → 排序 → 切页
        final List<Object> filterParams = appendWhere(sql, criteria, ds);

        // 排序（列排序）必须加在分页**之前**：分页是对排好序的结果切页，顺序反了就成了
        // 「先随便切一页再排这一页」，页内看着有序、页与页之间是乱的。
        // 未指定排序列时 appendOrderBy 什么都不追加 —— 语句与本改动之前逐字一致。
        criteria.appendOrderBy(sql, ds);
        ds.appendSqlLimit(sql, criteria);

        if (filterParams.isEmpty()) {
            // 无过滤条件：保留原来的 conn.query 路径，存量表格取数的语句与行为逐字不变
            ds.visitFirstConnection((conn) -> {
                try {
                    conn.query(sql.toString(), (rs) -> {
                        rows.add(readRow(rs, criteria));
                        // 兜底分页已在 sql 里由 appendSqlLimit 完成，这里读到尽头即可
                        return true;
                    });
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        } else {
            // 有过滤条件：值一律走 PreparedStatement 绑定，不拼进 SQL
            ds.visitFirstConnection((conn) -> {
                try (PreparedStatement statement = conn.preparedStatement(sql.toString())) {
                    for (int i = 0; i < filterParams.size(); i++) {
                        statement.setObject(i + 1, filterParams.get(i));
                    }
                    try (ResultSet rs = statement.executeQuery()) {
                        while (rs.next()) {
                            rows.add(readRow(rs, criteria));
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }
        return rows;
    }

    /**
     * 按 {@link DataSourceQueryCriteria#getFilters()} 渲染 WHERE 子句，并返回按占位符顺序排列的绑定值。
     *
     * <p>操作符与属性名的白名单已在 {@code DataSourceQueryCriteria#addFilter} 里校验过，
     * 这里只负责映射成 SQL 片段；{@code default} 分支仍然显式抛错，避免将来新增操作符时
     * 静默生成一条语义错误的语句。
     */
    private static List<Object> appendWhere(StringBuilder sql, DataSourceQueryCriteria criteria, DataSourceFactory ds) {
        List<ObjectTypeBinding.FilterCriterion> filters = criteria.getFilters();
        List<Object> params = new ArrayList<>();
        if (filters.isEmpty()) {
            return params;
        }
        sql.append(" WHERE ");
        for (int i = 0; i < filters.size(); i++) {
            if (i > 0) {
                sql.append(" AND ");
            }
            ObjectTypeBinding.FilterCriterion filter = filters.get(i);
            String escapedCol = ds.getEscapedEntity(filter.property());
            switch (filter.operator()) {
                case DataSourceQueryCriteria.OP_EQUALS -> {
                    sql.append(escapedCol).append("=?");
                    params.add(filter.value());
                }
                case DataSourceQueryCriteria.OP_CONTAINS -> {
                    sql.append(escapedCol).append(" LIKE ?");
                    params.add("%" + escapeLike(String.valueOf(filter.value())) + "%");
                }
                case DataSourceQueryCriteria.OP_GREATER_THAN -> {
                    sql.append(escapedCol).append(">?");
                    params.add(filter.value());
                }
                case DataSourceQueryCriteria.OP_LESS_THAN -> {
                    sql.append(escapedCol).append("<?");
                    params.add(filter.value());
                }
                case DataSourceQueryCriteria.OP_GREATER_THAN_OR_EQUAL -> {
                    sql.append(escapedCol).append(">=?");
                    params.add(filter.value());
                }
                case DataSourceQueryCriteria.OP_LESS_THAN_OR_EQUAL -> {
                    sql.append(escapedCol).append("<=?");
                    params.add(filter.value());
                }
                case DataSourceQueryCriteria.OP_IN -> {
                    Collection<?> values = toCollection(filter.value());
                    if (values.isEmpty()) {
                        throw new IllegalArgumentException("filter 'in' requires a non-empty collection for property:"
                                + filter.property());
                    }
                    sql.append(escapedCol).append(" IN (");
                    int index = 0;
                    for (Object value : values) {
                        if (index++ > 0) {
                            sql.append(',');
                        }
                        sql.append('?');
                        params.add(value);
                    }
                    sql.append(')');
                }
                default -> throw new IllegalArgumentException("illegal filter operator:" + filter.operator());
            }
        }
        return params;
    }

    /**
     * {@code LIKE} 的通配符转义：值里的 {@code \}、{@code %}、{@code _} 都按普通字符匹配。
     * MySQL / Doris / PostgreSQL 的 LIKE 默认转义符都是反斜杠，故不额外追加 ESCAPE 子句。
     */
    private static String escapeLike(String value) {
        return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_");
    }

    /**
     * {@code in} 的取值统一成集合：JSON 数组（fastjson JSONArray）、Java 集合、数组都接受。
     */
    private static Collection<?> toCollection(Object value) {
        if (value instanceof Collection<?> collection) {
            return collection;
        }
        if (value != null && value.getClass().isArray()) {
            int length = Array.getLength(value);
            List<Object> result = new ArrayList<>(length);
            for (int i = 0; i < length; i++) {
                result.add(Array.get(value, i));
            }
            return result;
        }
        throw new IllegalArgumentException("filter 'in' requires a collection value, but was:"
                + (value == null ? "null" : value.getClass().getName()));
    }

    private static RowMap readRow(ResultSet rs, DataSourceQueryCriteria criteria) throws SQLException {
        RowMap row = new RowMap();
        for (String col : criteria.getTargetCols()) {
            // 取原始类型（Integer/BigDecimal/Timestamp…）而不是 getString，
            // 需要字符串时由 RowMap.getColumn(key) 或调用方自行转换
            row.put(col, rs.getObject(col));
        }
        return row;
    }



    @Override
    public long countQuery(DataSourceQueryCriteria criteria) {
        DataSourceFactory ds = getDataSource();
        // 两种子句都不能带：ORDER BY 与总数无关；LIMIT（appendSqlLimit）更是会把「总数」变成
        // 「第一页的行数」—— 那样数出来的末页永远是第 1 页，翻页就此锁死。
        StringBuilder sql = new StringBuilder("select count(1) from ").append(escapedTableName(ds));
        final long[] total = new long[1];
        ds.visitFirstConnection((conn) -> {
            try {
                // query 的 callback 返回 false 即中断遍历；方法本身返回 boolean（是否有行），
                // 故用单元素数组把标量带出来。
                conn.query(sql.toString(), (rs) -> {
                    total[0] = rs.getLong(1);
                    return false;
                });
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        return total[0];
    }

    /**
     * count 与 select 两处共用的表名片段：抽出来只为让「两张语句查的是同一张表」这件事只有一个出处，
     * 免得改物理表名的回退逻辑时漏掉一处、变成 select 与 count 各查一张表。
     */
    private String escapedTableName(DataSourceFactory ds) {
        return ds.getEscapedEntity(this.physicalTableName);
    }

    @Override
    public List<ColumnMetaData> resolveTabCols() {
        try {
            return getDataSource().getTableMetadata(false, IPluginContext.getThreadLocalInstance(),
                    EntityName.parse(this.physicalTableName));
        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public String resolvePhysicalTableName(String objectTypeName) {
        return StringUtils.isBlank(this.physicalTableName) ? objectTypeName : this.physicalTableName;
    }

    @Override
    public ObjectTypeBindingInfo getObjectTypeBindingInfo() {
        final String dsName = getDataSource().name;
        if (getDataSource().getDescriptor() instanceof IEndTypeGetter endTypeGetter) {
            return new ObjectTypeBindingInfo(dsName, endTypeGetter.getEndType());
        }
        return new ObjectTypeBindingInfo(dsName, IEndTypeGetter.EndType.UnKnowStoreType);
    }

    @TISExtension
    public static class DftDesc extends Descriptor<ObjectTypeBinding> implements DescriptorUseableShortComment, IEndTypeGetter {
        public DftDesc() {
            super();
            this.valueChangePipe(KEY_DB_NAME, KEY_PHYSICAL_TABLE_NAME).render((meta, params) -> {
                String dbName = params.getString(KEY_DB_NAME);
                DataSourceFactory dataSource = DataSourceFactory.load(dbName);
                List<String> tabs = dataSource.getTablesInDB().getTabs();
                return tabs.stream().map((tab) -> new OptionWithEndType(tab, tab, IEndTypeGetter.EndType.Table)).toList();
            });
        }

        @Override
        public EndType getEndType() {
            return EndType.DataBase;
        }

        @Override
        public String shortComment() {
            return "绑定数据源";
        }

        @Override
        public String getDisplayName() {
            return KEY_DISPLAY_NAME;
        }
    }
}