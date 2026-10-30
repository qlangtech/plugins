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
import com.qlangtech.tis.plugin.ontology.AggregationFunc;
import com.qlangtech.tis.plugin.ontology.aggregation.AggregationCriteria;
import com.qlangtech.tis.plugin.ontology.aggregation.AggregationResult;
import com.qlangtech.tis.plugin.ontology.aggregation.GroupKey;
import com.qlangtech.tis.plugin.ontology.impl.objtype.aggregation.AggregationSqlBuilder;
import com.qlangtech.tis.plugin.ontology.impl.objtype.aggregation.AggregationSqlBuilders;
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
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
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
        final List<Object> filterParams = appendWhere(sql, criteria, ds, null);

        // 排序（列排序）必须加在分页**之前**：分页是对排好序的结果切页，顺序反了就成了
        // 「先随便切一页再排这一页」，页内看着有序、页与页之间是乱的。
        // 未指定排序列时 appendOrderBy 什么都不追加 —— 语句与本改动之前逐字一致。
        criteria.appendOrderBy(sql, ds);
        ds.appendSqlLimit(sql, criteria);

        visitQuery(ds, sql.toString(), filterParams, (rs) -> rows.add(readRow(rs, criteria)));
        return rows;
    }

    /**
     * 执行一条查询并把每一行交给 {@code rowConsumer} —— {@link #executeQuery} 与
     * {@link #countQuery} 共用的唯一执行路径。
     *
     * <p>为什么必须共用：过滤 → 计数 → 切页这三步对<b>同一个 criteria</b> 的解释必须完全一致。
     * 各写一份的话，很容易出现「行数按过滤后算、总数按全表算」这种只在翻到末页才暴露的错位。
     *
     * <p>两条分支的差异只在「有没有绑定参数」：无参数沿用 {@code conn.query}
     * （存量取数的语句与行为逐字不变，且它内部对 {@code JDBCConnection.class} 加了全局锁），
     * 有参数一律走 {@link PreparedStatement}，值不拼进 SQL。
     *
     * <p>⚠️ 两条分支的回调契约不同：{@code conn.query} 的 {@code ResultProcess} 返回 {@code false}
     * 才中断遍历，而 preparedStatement 分支是 {@code while (rs.next())}。这里把 {@code conn.query}
     * 的包装回调统一成<b>恒返回 true</b>（即两条分支都读到尽头）。对 {@code count(1)} 这种单行结果，
     * 读到尽头与「读一行就停」等价，count 的既有语义不变。
     */
    private static void visitQuery(DataSourceFactory ds, String sql, List<Object> params, RowConsumer rowConsumer) {
        if (params.isEmpty()) {
            // 无过滤条件：保留原来的 conn.query 路径，存量表格取数的语句与行为逐字不变
            ds.visitFirstConnection((conn) -> {
                try {
                    conn.query(sql, (rs) -> {
                        rowConsumer.accept(rs);
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
                try (PreparedStatement statement = conn.preparedStatement(sql)) {
                    for (int i = 0; i < params.size(); i++) {
                        statement.setObject(i + 1, params.get(i));
                    }
                    try (ResultSet rs = statement.executeQuery()) {
                        while (rs.next()) {
                            rowConsumer.accept(rs);
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    /**
     * 逐行处理一个 {@link ResultSet} 的当前位置。允许抛 {@link SQLException} —— 读取出错必须立刻
     * 终止，不能吞掉：那会让 count 拿到一个「看起来正常」的错误数字。
     */
    @FunctionalInterface
    private interface RowConsumer {
        void accept(ResultSet rs) throws SQLException;
    }

    /**
     * 数据库端聚合：把分组键 + 聚合函数下推成一条 {@code SELECT ... GROUP BY ...}。
     *
     * <p>与 {@link #executeQuery} 的差异：
     * <ul>
     *   <li>不分页（聚合结果已是分组行），也不下推排序 —— {@code orderByCol} 非空直接拒绝，
     *       静默忽略会让调用方误以为排序生效；</li>
     *   <li>只给 X 分组键加 {@code IS NOT NULL}，与「X 为空则跳过」的旧语义对齐；
     *       segment 为 NULL 仍作为一个分组；</li>
     *   <li>分组数超过 {@link AggregationCriteria#getMaxGroups()} 时只返回前 N 组并置
     *       {@link AggregationResult#truncated()}。</li>
     * </ul>
     *
     * <p>标识符在这里做最后一道白名单校验（{@link #resolveTabCols()}）：即使调用方绕过
     * 上层 planner，也不允许不存在的列进入 SQL。
     */
    @Override
    public AggregationResult executeAggregation(AggregationCriteria criteria) {
        Objects.requireNonNull(criteria, "aggregation criteria can not be null");
        if (criteria.getGroupKeys().isEmpty()) {
            throw new IllegalArgumentException("aggregation requires at least one group key");
        }
        if (criteria.getOrderByCol() != null) {
            throw new IllegalArgumentException("aggregation does not support orderByCol:'"
                    + criteria.getOrderByCol() + "', top-N must be resolved by caller after aggregation");
        }
        final DataSourceFactory ds = getDataSource();
        final AggregationSqlBuilder builder = AggregationSqlBuilders.resolve(ds);
        validateAggregationCols(criteria);

        final List<GroupKey> groupKeys = criteria.getGroupKeys();
        final List<AggregationCriteria.Metric> metrics = criteria.getMetrics();

        StringBuilder sql = new StringBuilder("select ");
        List<String> groupExprs = new ArrayList<>(groupKeys.size());
        for (int i = 0; i < groupKeys.size(); i++) {
            String groupExpr = builder.renderGroupKey(groupKeys.get(i), ds);
            groupExprs.add(groupExpr);
            if (i > 0) {
                sql.append(", ");
            }
            sql.append(groupExpr).append(" AS ").append(AggregationCriteria.groupAlias(i));
        }
        for (int i = 0; i < metrics.size(); i++) {
            AggregationCriteria.Metric metric = metrics.get(i);
            sql.append(", ").append(builder.renderMetric(metric.func(), metric.property(), ds))
                    .append(" AS ").append(AggregationCriteria.metricAlias(i));
        }
        sql.append(" from ").append(escapedTableName(ds));

        final List<Object> filterParams = appendWhere(sql, criteria, ds,
                "(" + groupExprs.get(0) + ") IS NOT NULL");

        sql.append(" GROUP BY ");
        for (int i = 0; i < groupExprs.size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }
            sql.append(i + 1);
        }
        // 多取一行用于判断是否超出上限；不走 appendSqlLimit（它依赖 page/pageSize，且 StarRocks 未实现）
        sql.append(" LIMIT ").append((long) criteria.getMaxGroups() + 1);

        final int groupCount = groupKeys.size();
        final int metricCount = metrics.size();
        final int maxGroups = criteria.getMaxGroups();
        final List<RowMap> rows = new ArrayList<>(Math.min(maxGroups, 1024));
        final boolean[] truncated = {false};
        ds.visitFirstConnection((conn) -> {
            try (PreparedStatement statement = conn.preparedStatement(sql.toString())) {
                for (int i = 0; i < filterParams.size(); i++) {
                    statement.setObject(i + 1, filterParams.get(i));
                }
                try (ResultSet rs = statement.executeQuery()) {
                    while (rs.next()) {
                        if (rows.size() >= maxGroups) {
                            truncated[0] = true;
                            break;
                        }
                        rows.add(readAggregationRow(rs, groupCount, metricCount));
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        return new AggregationResult(rows, truncated[0]);
    }

    /**
     * 聚合结果按<b>列序号</b>读取。
     *
     * <p>不复用 {@link #readRow}（它按 targetCols 取物理列名），也不用
     * {@code rs.getObject(alias)}：别名大小写规则在各方言 / 驱动上并不一致，取不到时
     * 只会得到 null 而不是报错。列序号由 SELECT 的固定顺序（分组键在前、指标在后）保证。
     *
     * <p>包级可见<b>为了可测</b>：用 {@link java.lang.reflect.Proxy} 造一个只实现
     * {@code getObject(int)} 的 {@link ResultSet} 即可覆盖，不必启动数据源。
     */
    static RowMap readAggregationRow(ResultSet rs, int groupCount, int metricCount) throws SQLException {
        RowMap row = new RowMap();
        for (int i = 0; i < groupCount; i++) {
            row.put(AggregationCriteria.groupAlias(i), rs.getObject(i + 1));
        }
        for (int i = 0; i < metricCount; i++) {
            row.put(AggregationCriteria.metricAlias(i), rs.getObject(groupCount + i + 1));
        }
        return row;
    }

    /**
     * 最后一道标识符白名单：分组键、指标、过滤条件里的列都必须在物理表的真实列里。
     */
    private void validateAggregationCols(AggregationCriteria criteria) {
        Set<String> tableCols = new HashSet<>();
        for (ColumnMetaData col : resolveTabCols()) {
            tableCols.add(col.getName());
        }
        if (tableCols.isEmpty()) {
            throw new IllegalStateException("table:" + this.physicalTableName + " has no column metadata");
        }
        for (GroupKey key : criteria.getGroupKeys()) {
            requireTableCol(tableCols, key.column(), "group key");
        }
        for (AggregationCriteria.Metric metric : criteria.getMetrics()) {
            if (metric.property() != null) {
                requireTableCol(tableCols, metric.property(), "metric");
            }
        }
        for (ObjectTypeBinding.FilterCriterion filter : criteria.getFilters()) {
            requireTableCol(tableCols, filter.property(), "filter");
        }
    }

    private void requireTableCol(Set<String> tableCols, String col, String what) {
        if (!tableCols.contains(col)) {
            throw new IllegalArgumentException("aggregation " + what + " column:'" + col
                    + "' does not exist in table:'" + this.physicalTableName + "', cols:" + tableCols);
        }
    }

    /**
     * 按 {@link DataSourceQueryCriteria#getFilters()} 渲染 WHERE 子句，并返回按占位符顺序排列的绑定值。
     *
     * <p>操作符与属性名的白名单已在 {@code DataSourceQueryCriteria#addFilter} 里校验过，
     * 这里只负责映射成 SQL 片段；{@code default} 分支仍然显式抛错，避免将来新增操作符时
     * 静默生成一条语义错误的语句。
     */
    private static List<Object> appendWhere(StringBuilder sql, DataSourceQueryCriteria criteria, DataSourceFactory ds,
                                            String trailingStaticPredicate) {
        List<ObjectTypeBinding.FilterCriterion> filters = criteria.getFilters();
        List<Object> params = new ArrayList<>();
        if (filters.isEmpty() && trailingStaticPredicate == null) {
            return params;
        }
        sql.append(" WHERE ");
        boolean appended = false;
        for (ObjectTypeBinding.FilterCriterion filter : filters) {
            if (appended) {
                sql.append(" AND ");
            }
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
            appended = true;
        }
        if (trailingStaticPredicate != null) {
            // 无 filters 时 WHERE 已经由上面输出；有 filters 时补一个 AND，避免悬空
            if (!filters.isEmpty()) {
                sql.append(" AND ");
            }
            sql.append(trailingStaticPredicate);
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



    /**
     * 与 {@link #executeQuery} <b>共用同一份 WHERE</b>：过滤之后的行数与过滤之后的总数必须来自
     * 同一个条件，否则分页条会按全表算、末页永远翻不到底。
     *
     * <p>两种子句仍然不能带：ORDER BY 与总数无关；LIMIT（appendSqlLimit）更是会把「总数」变成
     * 「第一页的行数」—— 那样数出来的末页永远是第 1 页，翻页就此锁死。
     */
    @Override
    public long countQuery(DataSourceQueryCriteria criteria) {
        DataSourceFactory ds = getDataSource();
        StringBuilder sql = new StringBuilder("select count(1) from ").append(escapedTableName(ds));
        final List<Object> filterParams = appendWhere(sql, criteria, ds, null);
        final long[] total = new long[1];
        // 单行结果，用单元素数组把标量带出来
        visitQuery(ds, sql.toString(), filterParams, (rs) -> total[0] = rs.getLong(1));
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