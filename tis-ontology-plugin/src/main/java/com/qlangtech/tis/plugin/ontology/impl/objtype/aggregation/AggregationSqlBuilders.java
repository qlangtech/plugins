package com.qlangtech.tis.plugin.ontology.impl.objtype.aggregation;

import com.qlangtech.tis.plugin.IEndTypeGetter;
import com.qlangtech.tis.plugin.ds.DataSourceFactory;
import com.qlangtech.tis.plugin.ontology.aggregation.AggregationNotSupportedException;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * 按数据源方言选择聚合 SQL 表达式实现。
 *
 * <p>不支持的方言直接抛 {@link AggregationNotSupportedException}，<b>不回落到内存聚合</b>。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/24
 */
public final class AggregationSqlBuilders {

    /**
     * 与 {@link #resolve(IEndTypeGetter.EndType)} 的分支保持一致；错误信息与文档都从这里取。
     */
    public static final List<IEndTypeGetter.EndType> SUPPORTED_END_TYPES = List.of(
            IEndTypeGetter.EndType.MySQL,
            IEndTypeGetter.EndType.MariaDB,
            IEndTypeGetter.EndType.TiDB,
            IEndTypeGetter.EndType.OceanBase,
            IEndTypeGetter.EndType.Doris,
            IEndTypeGetter.EndType.StarRocks);

    private static final AggregationSqlBuilder MYSQL = new MySqlAggregationSqlBuilder();
    private static final AggregationSqlBuilder MPP = new MppAggregationSqlBuilder();

    private AggregationSqlBuilders() {
    }

    public static AggregationSqlBuilder resolve(DataSourceFactory ds) {
        Objects.requireNonNull(ds, "dataSourceFactory can not be null");
        IEndTypeGetter.EndType endType = ds.getDescriptor() instanceof IEndTypeGetter getter
                ? getter.getEndType() : null;
        return resolve(endType, ds.getDescriptor().getDisplayName());
    }

    public static AggregationSqlBuilder resolve(IEndTypeGetter.EndType endType) {
        return resolve(endType, null);
    }

    private static AggregationSqlBuilder resolve(IEndTypeGetter.EndType endType, String dsName) {
        if (endType == null) {
            throw new AggregationNotSupportedException("data source:'" + dsName
                    + "' has no EndType, database side aggregation is not supported; supported:"
                    + supportedTypes());
        }
        return switch (endType) {
            case MySQL, MariaDB, TiDB, OceanBase -> MYSQL;
            case Doris, StarRocks -> MPP;
            default -> throw new AggregationNotSupportedException("data source:'" + dsName
                    + "' endType:'" + endType.getVal() + "' does not support database side aggregation; supported:"
                    + supportedTypes());
        };
    }

    /**
     * 已支持方言的 token 列表，用于错误信息与文档。
     */
    public static String supportedTypes() {
        return SUPPORTED_END_TYPES.stream()
                .map(IEndTypeGetter.EndType::getVal)
                .collect(Collectors.joining("/"));
    }
}
