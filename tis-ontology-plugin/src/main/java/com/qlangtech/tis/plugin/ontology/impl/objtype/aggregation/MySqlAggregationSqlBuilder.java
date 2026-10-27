package com.qlangtech.tis.plugin.ontology.impl.objtype.aggregation;

import com.qlangtech.tis.plugin.ds.IDBReservedKeys;
import com.qlangtech.tis.plugin.ontology.AggregationFunc;
import com.qlangtech.tis.plugin.ontology.aggregation.AggregationNotSupportedException;
import com.qlangtech.tis.plugin.ontology.aggregation.GroupKey;

/**
 * MySQL 系方言的分组 / 聚合表达式。
 *
 * <h3>时间分桶为什么返回整数序号</h3>
 * 不返回格式化日期字符串：{@code STR_TO_DATE} / {@code DATE_FORMAT} 的格式符、时区、
 * 以及 {@code java.sql.Date.toInstant()} 抛异常等坑会在方言之间反复出现。这里只用
 * {@code YEAR/MONTH/DATEDIFF/TIMESTAMPDIFF/FLOOR} 计算「第几个桶」，调用方拿到数字后用
 * {@code bucketStart} 还原成日期 —— 数值计算没有格式符，也不依赖会话时区做字符串转换。
 *
 * <p>不使用 {@code DIV}：StarRocks 文档未承诺整数除法运算符，{@code FLOOR(x / u)} 在
 * MySQL / Doris / StarRocks 上语义一致。
 *
 * <p>锚点与 Java 侧对齐：日 / 时 / 分 / 秒以 1970-01-01 为 0，周以 1970-01-05（周一）为 0。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/24
 */
public class MySqlAggregationSqlBuilder implements AggregationSqlBuilder {

    private static final String DAY_EPOCH = "'1970-01-01'";
    private static final String WEEK_EPOCH = "'1970-01-05'";
    private static final String DATETIME_EPOCH = "'1970-01-01 00:00:00'";

    @Override
    public String renderGroupKey(GroupKey key, IDBReservedKeys reservedKeys) {
        final String col = reservedKeys.getEscapedEntity(key.column());
        if (!key.isTemporal()) {
            return col;
        }
        final int unit = Math.max(1, key.unitValue());
        return switch (key.granularity()) {
            case YEAR ->
                    "FLOOR(YEAR(" + col + ") / " + unit + ") * " + unit;
            case QUARTER ->
                    "FLOOR((YEAR(" + col + ") * 4 + FLOOR((MONTH(" + col + ") - 1) / 3)) / " + unit + ") * " + unit;
            case MONTH ->
                    "FLOOR((YEAR(" + col + ") * 12 + (MONTH(" + col + ") - 1)) / " + unit + ") * " + unit;
            case WEEK ->
                    "FLOOR(DATEDIFF(" + col + ", " + WEEK_EPOCH + ") / " + (7L * unit) + ") * " + unit;
            case DAY ->
                    "FLOOR(DATEDIFF(" + col + ", " + DAY_EPOCH + ") / " + unit + ") * " + unit;
            case HOUR ->
                    "FLOOR(TIMESTAMPDIFF(SECOND, " + DATETIME_EPOCH + ", " + col + ") / " + (3600L * unit) + ") * " + unit;
            case MINUTE ->
                    "FLOOR(TIMESTAMPDIFF(SECOND, " + DATETIME_EPOCH + ", " + col + ") / " + (60L * unit) + ") * " + unit;
            case SECOND ->
                    "FLOOR(TIMESTAMPDIFF(SECOND, " + DATETIME_EPOCH + ", " + col + ") / " + unit + ") * " + unit;
        };
    }

    @Override
    public String renderMetric(AggregationFunc func, String property, IDBReservedKeys reservedKeys) {
        if (func == AggregationFunc.COUNT) {
            return "COUNT(*)";
        }
        final String col = reservedKeys.getEscapedEntity(property);
        return switch (func) {
            case SUM -> "SUM(" + col + ")";
            case AVG -> "AVG(" + col + ")";
            case MIN -> "MIN(" + col + ")";
            case MAX -> "MAX(" + col + ")";
            case COUNT_DISTINCT -> "COUNT(DISTINCT " + col + ")";
            case APPROX_CARDINALITY -> renderApproxCardinality(col);
            case COLLECT_LIST, COLLECT_SET -> throw new AggregationNotSupportedException(
                    "aggregation:" + func + " returns an array and has no scalar semantics in AggregationResult");
            case COUNT -> throw new IllegalStateException("unreachable, COUNT is handled above");
        };
    }

    /**
     * 近似去重。MySQL 系没有近似函数，回落到精确 {@code COUNT(DISTINCT)}；
     * MPP 方言覆盖本方法。
     */
    protected String renderApproxCardinality(String escapedCol) {
        return "COUNT(DISTINCT " + escapedCol + ")";
    }
}
