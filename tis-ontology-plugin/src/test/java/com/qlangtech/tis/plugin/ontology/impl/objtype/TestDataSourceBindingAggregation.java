package com.qlangtech.tis.plugin.ontology.impl.objtype;

import com.qlangtech.tis.ibatis.RowMap;
import com.qlangtech.tis.plugin.ds.IDBReservedKeys;
import com.qlangtech.tis.plugin.ontology.AggregationFunc;
import com.qlangtech.tis.plugin.ontology.aggregation.AggregationCriteria;
import com.qlangtech.tis.plugin.ontology.aggregation.GroupKey;
import com.qlangtech.tis.plugin.ontology.impl.objtype.FilterOperator;
import com.qlangtech.tis.plugin.ontology.impl.objtype.ObjectTypeBinding.DataSourceQueryCriteria;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Proxy;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;


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

    // ------------------------------------------------------------------
    // 可渲染性闸门：合法形态有三种，非法形态只有一种
    // ------------------------------------------------------------------

    /**
     * 只有分组键、没有指标 —— 候选项的 distinct 查询形态（{@code ObjectSetOpsSource}）。
     *
     * <p>它曾被 {@code metrics.isEmpty()} 的闸门拦掉，且拦得<b>静默</b>：候选项解析的 catch
     * 把异常降级成空列表，界面上只表现为「下拉里没有选项」。
     */
    @Test
    public void groupKeysWithoutMetricIsRenderable() {
        AggregationCriteria criteria = new AggregationCriteria();
        criteria.addTargetCol("dept");
        criteria.addGroupKey(GroupKey.rawColumn("dept"));

        DataSourceBinding.requireRenderable(criteria);
    }

    /** 只有指标、没有分组键 —— 单行标量聚合（直方图取 MIN/MAX 定桶宽）。 */
    @Test
    public void metricWithoutGroupKeyIsRenderable() {
        AggregationCriteria criteria = new AggregationCriteria();
        criteria.addTargetCol("amount");
        criteria.addMetric(AggregationFunc.MIN, "amount");

        DataSourceBinding.requireRenderable(criteria);
    }

    /** 两者都空会渲染成 {@code select from t}，各方言都不合法。 */
    @Test
    public void emptyCriteriaIsRejected() {
        try {
            DataSourceBinding.requireRenderable(new AggregationCriteria());
            Assert.fail("分组键与指标都为空时不能渲染出合法 SQL");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue("报错要能指认缺的是什么，实际:" + e.getMessage(),
                    e.getMessage().contains("group key or metric"));
        }
    }

    // ------------------------------------------------------------------
    // 空值分组：默认加 IS NOT NULL，显式要空值分组时不能加
    // ------------------------------------------------------------------

    /**
     * 默认形态：第一个分组键加 {@code IS NOT NULL}，与「X 为空则跳过该行」的旧语义一致。
     *
     * <p>这条谓词是<b>静默</b>的：多加了只会让分组里少一行（界面上少一个候选值），
     * 少加了则会让「空值」混进候选列表里且没有任何标记。
     */
    @Test
    public void nullGroupPredicateAddedByDefault() {
        AggregationCriteria criteria = new AggregationCriteria();
        Assert.assertEquals("(`dept`) IS NOT NULL",
                DataSourceBinding.nonNullGroupPredicate(criteria, List.of("`dept`")));
    }

    /** 候选项的 {@code No value} 行要的是「空值那一组」，故显式关掉 {@code IS NOT NULL}。 */
    @Test
    public void nullGroupPredicateSkippedWhenNullGroupsRequested() {
        AggregationCriteria criteria = new AggregationCriteria();
        criteria.setIncludeNullGroups(true);
        Assert.assertNull("要了空值分组就不能再加 IS NOT NULL，否则那一组永远查不到",
                DataSourceBinding.nonNullGroupPredicate(criteria, List.of("`dept`")));
    }

    /** 标量聚合（直方图取 MIN/MAX 那一趟）没有分组列可判空。 */
    @Test
    public void nullGroupPredicateAbsentForScalarAggregation() {
        AggregationCriteria criteria = new AggregationCriteria();
        criteria.addTargetCol("amount");
        criteria.addMetric(AggregationFunc.MIN, "amount");
        Assert.assertNull(DataSourceBinding.nonNullGroupPredicate(criteria, List.of()));
    }

    // ------------------------------------------------------------------
    // 谓词渲染：isNull / inOrNull
    // ------------------------------------------------------------------

    /** {@code isNull} 不带比较值，也就不绑定任何参数。 */
    @Test
    public void isNullRendersWithoutParameter() {
        DataSourceQueryCriteria criteria = criteriaWithFilter("dept", FilterOperator.isNull, null);
        Rendered rendered = render(criteria);

        Assert.assertEquals("select 1 from t WHERE `dept` IS NULL", rendered.sql);
        Assert.assertTrue("IS NULL 没有绑定值，多绑一个会让占位符与参数错位",
                rendered.params.isEmpty());
    }

    /**
     * {@code inOrNull} 的括号必须把整个 OR 包住。
     *
     * <p>少了外层括号时 {@code a AND b IN (?,?) OR c IS NULL} 会被解析成
     * {@code (a AND b IN (?,?)) OR c IS NULL} —— 前面所有过滤条件对空值行统统失效，
     * 而且不报错。
     */
    @Test
    public void inOrNullRendersOrWithParentheses() {
        DataSourceQueryCriteria criteria = criteriaWithFilter("dept", FilterOperator.inOrNull, List.of("A", "B"));
        Rendered rendered = render(criteria);

        Assert.assertEquals("select 1 from t WHERE (`dept` IN (?,?) OR `dept` IS NULL)", rendered.sql);
        Assert.assertEquals(List.of("A", "B"), rendered.params);
    }

    /**
     * {@code inOrNull} 的空集合退化成 {@code IS NULL}：{@code IN ()} 在各方言都不合法，
     * 而「属于空集合或者为空」在语义上就等于「为空」。
     */
    @Test
    public void inOrNullWithEmptyCollectionDegradesToIsNull() {
        DataSourceQueryCriteria criteria = criteriaWithFilter("dept", FilterOperator.inOrNull, List.of());
        Rendered rendered = render(criteria);

        Assert.assertEquals("select 1 from t WHERE `dept` IS NULL", rendered.sql);
        Assert.assertTrue(rendered.params.isEmpty());
    }

    /** 两个空值算子可以共存于同一 criteria；未登记的算子串在解析期就被拒。 */
    @Test
    public void nullOperatorsAreAccepted() {
        DataSourceQueryCriteria criteria = new DataSourceQueryCriteria();
        criteria.addTargetCol("dept");
        criteria.addFilter("dept", FilterOperator.isNull, null);
        criteria.addFilter("dept", FilterOperator.inOrNull, List.of("A"));
        Assert.assertEquals(2, criteria.getFilters().size());

        try {
            FilterOperator.parse("isNotNull");
            Assert.fail("未登记的算子必须被拒");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("illegal filter operator"));
        }
    }

    /** {@code inOrNull} 与 {@code in} 一样要求集合值（空集合可以，标量不行）。 */
    @Test
    public void inOrNullRejectsScalarValue() {
        DataSourceQueryCriteria criteria = new DataSourceQueryCriteria();
        criteria.addTargetCol("dept");
        try {
            criteria.addFilter("dept", FilterOperator.inOrNull, "A");
            Assert.fail("inOrNull 的标量值必须被拒：后端无法把它渲染成集合");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("requires a collection value"));
        }
    }

    // ------------------------------------------------------------------

    private record Rendered(String sql, List<Object> params) {
    }

    private static DataSourceQueryCriteria criteriaWithFilter(String col, FilterOperator operator, Object value) {
        DataSourceQueryCriteria criteria = new DataSourceQueryCriteria();
        criteria.addTargetCol(col);
        criteria.addFilter(col, operator, value);
        return criteria;
    }

    /**
     * 渲染 WHERE 并补上 SELECT 前缀 —— 被测的只有 WHERE 片段本身。
     *
     * <p>标识符转义用反引号桩：真实数据源的转义规则随方言变化，而本用例断言的是
     * 「算子被映射成什么 SQL 形状」，不该被转义符的差异淹没。
     */
    private static Rendered render(DataSourceQueryCriteria criteria) {
        StringBuilder sql = new StringBuilder("select 1 from t");
        List<Object> params = DataSourceBinding.appendWhere(sql, criteria, BACKTICK, null);
        return new Rendered(sql.toString(), params);
    }

    private static final IDBReservedKeys BACKTICK = new IDBReservedKeys() {
        @Override
        public java.util.Optional<String> getEscapeChar() {
            return java.util.Optional.of("`");
        }
    };

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
