package com.qlangtech.tis.plugin.ontology.impl.objtype;

import com.qlangtech.tis.ibatis.RowMap;
import com.qlangtech.tis.plugin.ontology.aggregation.AggregationCriteria;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Proxy;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * 聚合结果行的读取契约。
 *
 * <p>按<b>列序号</b>读取，而不是按别名：Java / MySQL / Doris / StarRocks 对别名大小写的
 * 处理并不一致，按别名取值取不到时只会得到 null，图表上表现为「数据凭空消失」。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/24
 */
public class TestDataSourceBindingAggregation {

    @Test
    public void readsGroupsThenMetricsByPosition() throws SQLException {
        ResultSet rs = rs("A", "north", 3L, 12.5d);
        RowMap row = DataSourceBinding.readAggregationRow(rs, 2, 2);

        Assert.assertEquals("A", row.get(AggregationCriteria.GROUP_ALIAS));
        Assert.assertEquals("north", row.get(AggregationCriteria.SEGMENT_ALIAS));
        Assert.assertEquals(3L, row.get(AggregationCriteria.metricAlias(0)));
        Assert.assertEquals(12.5d, row.get(AggregationCriteria.metricAlias(1)));
    }

    @Test
    public void readsOnlyTheColumnsThatExist() throws SQLException {
        // 无 segment、单指标：SELECT 只有两列，第 3 列不存在
        ResultSet rs = rs("A", 3L);
        RowMap row = DataSourceBinding.readAggregationRow(rs, 1, 1);

        Assert.assertEquals("A", row.get(AggregationCriteria.GROUP_ALIAS));
        Assert.assertEquals(3L, row.get(AggregationCriteria.metricAlias(0)));
        Assert.assertFalse(row.containsKey(AggregationCriteria.SEGMENT_ALIAS));
    }

    @Test
    public void nullMetricIsPreserved() throws SQLException {
        ResultSet rs = rs("A", null);
        RowMap row = DataSourceBinding.readAggregationRow(rs, 1, 1);

        Assert.assertTrue(row.containsKey(AggregationCriteria.metricAlias(0)));
        Assert.assertNull("全 NULL 分组是缺口，不是 0", row.get(AggregationCriteria.metricAlias(0)));
    }

    /**
     * 只实现 {@code getObject(int)} 的 {@link ResultSet} 桩：其余方法一律抛错，
     * 这样「实现里偷偷按别名/元数据取值」会立刻暴露。
     */
    private static ResultSet rs(Object... values) {
        return (ResultSet) Proxy.newProxyInstance(
                TestDataSourceBindingAggregation.class.getClassLoader(),
                new Class<?>[]{ResultSet.class},
                (proxy, method, args) -> {
                    if ("getObject".equals(method.getName()) && args != null && args.length == 1
                            && args[0] instanceof Integer index) {
                        if (index < 1 || index > values.length) {
                            throw new SQLException("column index out of range:" + index);
                        }
                        return values[index - 1];
                    }
                    throw new UnsupportedOperationException("unexpected ResultSet method:" + method.getName());
                });
    }
}
