package com.qlangtech.tis.plugin.datax.transformer.impl.joiner;

import com.alibaba.datax.common.element.ColumnAwareRecord;
import com.qlangtech.tis.plugin.ds.DataType;
import com.qlangtech.tis.plugin.table.join.TableJoinFilterCondition;
import com.qlangtech.tis.plugin.table.join.TableJoinFilterConditionCreatorFactory;
import com.qlangtech.tis.plugin.table.join.TableJoinMatchCondition;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * 构造 JoinCacheKey，并对主表/维度表两侧的 join key 值做类型归一化。
 * <p>
 * DataX record 与 JDBC ResultSet 对同一逻辑值可能返回不同 Java 类型（如 MySQL INT 主表侧可能是 Long，
 * 维度侧是 Integer）。直接以原对象做 HashMap key 会导致静默 miss，因此必须按列类型统一归一化。
 * </p>
 *
 * @author 百岁 (baisui@qlangtech.com)
 */
public class JoinCacheKeyBuilder {

    private final List<Function<Object, Object>> matchNormalizers;
    private final List<TableJoinFilterCondition> dimFilters;

    public JoinCacheKeyBuilder(List<TableJoinMatchCondition> matchCondition,
                               List<TableJoinFilterCondition> filterConditions) {
        Objects.requireNonNull(matchCondition, "matchCondition can not be null");
        this.matchNormalizers = new ArrayList<>(matchCondition.size());
        for (TableJoinMatchCondition mc : matchCondition) {
            this.matchNormalizers.add(createNormalizer(mc));
        }

        this.dimFilters = new ArrayList<>();
        if (filterConditions != null) {
            for (TableJoinFilterCondition fc : filterConditions) {
                if (fc.getTableType() == TableJoinFilterConditionCreatorFactory.TableType.Dimension) {
                    this.dimFilters.add(fc);
                }
            }
        }
    }

    /**
     * 从主表记录构造 key
     */
    public TargetRowsCache.JoinCacheKey buildFromRecord(ColumnAwareRecord record,
                                                         List<TableJoinMatchCondition> matchCondition) {
        TargetRowsCache.JoinCacheKey key = new TargetRowsCache.JoinCacheKey();
        for (int i = 0; i < matchCondition.size(); i++) {
            TableJoinMatchCondition mc = matchCondition.get(i);
            Object raw = record.getColumn(mc.getPrimaryTableMatchColName());
            key.addParam(mc.getDimensionMatchColName())
               .addPrimaryVal(matchNormalizers.get(i).apply(raw));
        }
        appendDimFilterParams(key);
        return key;
    }

    /**
     * 从维度表 ResultSet 构造 key，match 列必须位于 SELECT 列表最前面且顺序与 matchCondition 一致
     */
    public TargetRowsCache.JoinCacheKey buildFromResultSet(ResultSet rs,
                                                            List<TableJoinMatchCondition> matchCondition) throws SQLException {
        TargetRowsCache.JoinCacheKey key = new TargetRowsCache.JoinCacheKey();
        for (int i = 0; i < matchCondition.size(); i++) {
            TableJoinMatchCondition mc = matchCondition.get(i);
            Object raw = rs.getObject(i + 1);
            key.addParam(mc.getDimensionMatchColName())
               .addPrimaryVal(matchNormalizers.get(i).apply(raw));
        }
        appendDimFilterParams(key);
        return key;
    }

    private void appendDimFilterParams(TargetRowsCache.JoinCacheKey key) {
        for (TableJoinFilterCondition fc : dimFilters) {
            key.addParam(fc.getColumnName()).addParam(fc.getValue());
        }
    }

    private static Function<Object, Object> createNormalizer(TableJoinMatchCondition mc) {
        DataType type = Objects.requireNonNull(mc.getDimensionMatchColType(),
                "dimensionMatchColType can not be null for col:" + mc.getDimensionMatchColName());
        return type.accept(new DataType.PartialTypeVisitor<Function<Object, Object>>() {
            @Override
            public Function<Object, Object> bigInt(DataType type) {
                return v -> v == null ? null : ((Number) v).longValue();
            }

            @Override
            public Function<Object, Object> doubleType(DataType type) {
                return v -> v == null ? null : ((Number) v).doubleValue();
            }

            @Override
            public Function<Object, Object> bitType(DataType type) {
                return v -> v == null ? null : ((Number) v).longValue();
            }

            @Override
            public Function<Object, Object> varcharType(DataType type) {
                return v -> v == null ? null : String.valueOf(v);
            }

            @Override
            public Function<Object, Object> dateType(DataType type) {
                return Function.identity();
            }

            @Override
            public Function<Object, Object> timestampType(DataType type) {
                return Function.identity();
            }

            @Override
            public Function<Object, Object> blobType(DataType type) {
                return Function.identity();
            }
        });
    }
}
