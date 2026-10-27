package com.qlangtech.tis.plugin.ontology.impl.objtype.aggregation;

/**
 * Doris / StarRocks 等 MPP 方言：分桶公式与 MySQL 系相同，近似去重用原生函数。
 *
 * <p>{@code APPROX_COUNT_DISTINCT} 的结果是统计估计，不再与精确去重逐值一致；
 * 这是「大表上能用」的必要取舍。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/24
 */
public class MppAggregationSqlBuilder extends MySqlAggregationSqlBuilder {

    @Override
    protected String renderApproxCardinality(String escapedCol) {
        return "APPROX_COUNT_DISTINCT(" + escapedCol + ")";
    }
}
