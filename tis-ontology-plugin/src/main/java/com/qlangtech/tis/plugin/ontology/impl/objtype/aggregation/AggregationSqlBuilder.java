package com.qlangtech.tis.plugin.ontology.impl.objtype.aggregation;

import com.qlangtech.tis.plugin.ds.IDBReservedKeys;
import com.qlangtech.tis.plugin.ontology.AggregationFunc;
import com.qlangtech.tis.plugin.ontology.aggregation.AggregationNotSupportedException;
import com.qlangtech.tis.plugin.ontology.aggregation.GroupKey;

/**
 * 把结构化的分组键 / 指标渲染成某种数据库方言的 SQL 表达式。
 *
 * <p>实现方只负责「表达式」，不负责 SELECT / WHERE / GROUP BY / LIMIT 的骨架 ——
 * 骨架（别名、参数绑定、截断）由 {@code DataSourceBinding.executeAggregation} 统一拼装，
 * 保证各方言除了表达式差异之外行为一致。
 *
 * <h3>为什么入参是 {@link IDBReservedKeys} 而不是 DataSourceFactory</h3>
 * 单测里可以用一个只做标识符转义的桩，不需要启动真实数据源。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/24
 */
public interface AggregationSqlBuilder {

    /**
     * 渲染分组键表达式。时间分桶返回<b>对齐后的整数序号</b>（不是格式化日期），
     * 由调用方用 {@code bucketStart} 还原桶起点。
     */
    String renderGroupKey(GroupKey key, IDBReservedKeys reservedKeys);

    /**
     * 渲染聚合表达式。
     *
     * @param property 参与聚合的列；{@link AggregationFunc#COUNT} 时为 {@code null}
     * @throws AggregationNotSupportedException 该函数在当前方言下没有标量语义
     */
    String renderMetric(AggregationFunc func, String property, IDBReservedKeys reservedKeys);
}
