package com.qlangtech.tis.plugin.ontology.impl.objtype.aggregation;

import com.qlangtech.tis.plugin.IEndTypeGetter;
import com.qlangtech.tis.plugin.ds.IDBReservedKeys;
import com.qlangtech.tis.plugin.ontology.AggregationFunc;
import com.qlangtech.tis.plugin.ontology.aggregation.AggregationNotSupportedException;
import com.qlangtech.tis.plugin.ontology.aggregation.GroupKey;
import com.qlangtech.tis.plugin.ontology.aggregation.TimeGranularity;
import org.junit.Assert;
import org.junit.Test;

import java.util.Optional;

/**
 * 各方言的表达式逐字断言。
 *
 * <p>SQL 文本是前端看不懂、运行期也不会报错的那一类东西：公式悄悄写错时，图表只是
 * 「数不对」。因此这里对 8 种粒度 × unitValue、6 种指标、方言差异逐字断言。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/24
 */
public class TestAggregationSqlBuilder {

    private static final String COL = "created_at";
    private static final String ESCAPED = "`created_at`";

    /** 只做标识符转义的桩，避免启动真实数据源。 */
    private static final IDBReservedKeys KEYS = new IDBReservedKeys() {
        @Override
        public Optional<String> getEscapeChar() {
            return Optional.of("`");
        }
    };

    private static final AggregationSqlBuilder MYSQL = new MySqlAggregationSqlBuilder();
    private static final AggregationSqlBuilder MPP = new MppAggregationSqlBuilder();

    // ------------------------------------------------------------------
    // 分组键（时间分桶返回整数序号）
    // ------------------------------------------------------------------

    @Test
    public void rawColumnIsJustEscaped() {
        Assert.assertEquals(ESCAPED, MYSQL.renderGroupKey(GroupKey.rawColumn(COL), KEYS));
    }

    @Test
    public void yearAndQuarter() {
        Assert.assertEquals("FLOOR(YEAR(" + ESCAPED + ") / 1) * 1",
                MYSQL.renderGroupKey(temporal(TimeGranularity.YEAR, 1), KEYS));
        Assert.assertEquals("FLOOR(YEAR(" + ESCAPED + ") / 7) * 7",
                MYSQL.renderGroupKey(temporal(TimeGranularity.YEAR, 7), KEYS));
        Assert.assertEquals("FLOOR((YEAR(" + ESCAPED + ") * 4 + FLOOR((MONTH(" + ESCAPED + ") - 1) / 3)) / 3) * 3",
                MYSQL.renderGroupKey(temporal(TimeGranularity.QUARTER, 3), KEYS));
    }

    @Test
    public void month() {
        Assert.assertEquals("FLOOR((YEAR(" + ESCAPED + ") * 12 + (MONTH(" + ESCAPED + ") - 1)) / 1) * 1",
                MYSQL.renderGroupKey(temporal(TimeGranularity.MONTH, 1), KEYS));
        Assert.assertEquals("FLOOR((YEAR(" + ESCAPED + ") * 12 + (MONTH(" + ESCAPED + ") - 1)) / 7) * 7",
                MYSQL.renderGroupKey(temporal(TimeGranularity.MONTH, 7), KEYS));
    }

    @Test
    public void weekAnchorsAtMonday() {
        Assert.assertEquals("FLOOR(DATEDIFF(" + ESCAPED + ", '1970-01-05') / 7) * 1",
                MYSQL.renderGroupKey(temporal(TimeGranularity.WEEK, 1), KEYS));
        // unitValue=2 → 两周一个桶 → 除数 14
        Assert.assertEquals("FLOOR(DATEDIFF(" + ESCAPED + ", '1970-01-05') / 14) * 2",
                MYSQL.renderGroupKey(temporal(TimeGranularity.WEEK, 2), KEYS));
    }

    @Test
    public void day() {
        Assert.assertEquals("FLOOR(DATEDIFF(" + ESCAPED + ", '1970-01-01') / 7) * 7",
                MYSQL.renderGroupKey(temporal(TimeGranularity.DAY, 7), KEYS));
    }

    @Test
    public void subDayGranularitiesUseSecondsThenDivide() {
        Assert.assertEquals("FLOOR(TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', " + ESCAPED + ") / 7200) * 2",
                MYSQL.renderGroupKey(temporal(TimeGranularity.HOUR, 2), KEYS));
        Assert.assertEquals("FLOOR(TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', " + ESCAPED + ") / 120) * 2",
                MYSQL.renderGroupKey(temporal(TimeGranularity.MINUTE, 2), KEYS));
        Assert.assertEquals("FLOOR(TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', " + ESCAPED + ") / 30) * 30",
                MYSQL.renderGroupKey(temporal(TimeGranularity.SECOND, 30), KEYS));
    }

    @Test
    public void unitValueBelowOneIsRejectedAtConstruction() {
        // 除以 0 的分桶公式在 SQL 里不会报出可读错误，必须在核心 API 构造期就拒绝
        try {
            new GroupKey(COL, TimeGranularity.DAY, 0);
            Assert.fail("unitValue 必须 >= 1");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("unitValue"));
        }
    }

    @Test
    public void mppBucketingIsIdenticalToMySql() {
        for (TimeGranularity granularity : TimeGranularity.values()) {
            Assert.assertEquals("MPP 的分桶公式必须与 MySQL 系一致：" + granularity,
                    MYSQL.renderGroupKey(temporal(granularity, 3), KEYS),
                    MPP.renderGroupKey(temporal(granularity, 3), KEYS));
        }
    }

    /**
     * 数值分桶只产出桶序号 {@code FLOOR(col / width)}，<b>没有</b> {@code * width} 的还原。
     *
     * <p>与时间分桶的差别是刻意的：时间分桶把序号乘回成时间戳（图表横轴要能读），
     * 数值分桶的序号直接就是数组下标，还原成区间起点由
     * {@code HistogramDataProcessor} 在 Java 侧做（那里的桶边界要按 {@code bucketCount}
     * 折进末桶，SQL 侧做不到）。若这里顺手也乘一次，末桶折叠会整体错位一格。
     */
    @Test
    public void numericGroupKeyRendersBareFloor() {
        // toPlainString 保留 BigDecimal.valueOf 给出的标度，`100.0` 与 `100` 在 SQL 侧等价
        Assert.assertEquals("FLOOR(" + ESCAPED + " / 100.0)",
                MYSQL.renderGroupKey(GroupKey.numeric(COL, 100d, 1), KEYS));
        Assert.assertEquals("MPP 与 MySQL 系一致",
                MYSQL.renderGroupKey(GroupKey.numeric(COL, 7.5d, 1), KEYS),
                MPP.renderGroupKey(GroupKey.numeric(COL, 7.5d, 1), KEYS));
    }

    /**
     * 桶宽用<b>平铺小数</b>渲染，不用 {@code Double.toString}。
     *
     * <p>极小的桶宽在 {@code Double.toString} 下会变成 {@code 1.0E-20}：科学计数法在各方言里
     * 的接受程度不一致，而 {@code toPlainString} 在全部受支持方言上都合法。
     */
    @Test
    public void numericBucketWidthIsPlainDecimal() {
        String rendered = MYSQL.renderGroupKey(GroupKey.numeric(COL, 1.0E-20d, 1), KEYS);
        Assert.assertFalse("桶宽不能渲染成科学计数法：" + rendered, rendered.contains("E"));
        Assert.assertTrue("桶宽必须是平铺小数：" + rendered,
                rendered.startsWith("FLOOR(" + ESCAPED + " / 0.0000000000000000000"));

        // 大桶宽同理：Double.toString 会给 1.0E20
        Assert.assertEquals("FLOOR(" + ESCAPED + " / 100000000000000000000)",
                MYSQL.renderGroupKey(GroupKey.numeric(COL, 1.0E20d, 1), KEYS));
    }

    // ------------------------------------------------------------------
    // 指标
    // ------------------------------------------------------------------

    @Test
    public void scalarMetrics() {
        Assert.assertEquals("COUNT(*)", MYSQL.renderMetric(AggregationFunc.COUNT, null, KEYS));
        Assert.assertEquals("SUM(" + ESCAPED + ")", MYSQL.renderMetric(AggregationFunc.SUM, COL, KEYS));
        Assert.assertEquals("AVG(" + ESCAPED + ")", MYSQL.renderMetric(AggregationFunc.AVG, COL, KEYS));
        Assert.assertEquals("MIN(" + ESCAPED + ")", MYSQL.renderMetric(AggregationFunc.MIN, COL, KEYS));
        Assert.assertEquals("MAX(" + ESCAPED + ")", MYSQL.renderMetric(AggregationFunc.MAX, COL, KEYS));
        Assert.assertEquals("COUNT(DISTINCT " + ESCAPED + ")",
                MYSQL.renderMetric(AggregationFunc.COUNT_DISTINCT, COL, KEYS));
    }

    @Test
    public void approxCardinalityDiffersByDialect() {
        // MySQL 系没有近似函数，回落精确去重
        Assert.assertEquals("COUNT(DISTINCT " + ESCAPED + ")",
                MYSQL.renderMetric(AggregationFunc.APPROX_CARDINALITY, COL, KEYS));
        // MPP 用原生近似函数，结果不再逐值精确 —— 这是大表上能用的必要取舍
        Assert.assertEquals("APPROX_COUNT_DISTINCT(" + ESCAPED + ")",
                MPP.renderMetric(AggregationFunc.APPROX_CARDINALITY, COL, KEYS));
    }

    @Test
    public void collectFunctionsAreRejected() {
        assertUnsupported(AggregationFunc.COLLECT_LIST);
        assertUnsupported(AggregationFunc.COLLECT_SET);
    }

    private static void assertUnsupported(AggregationFunc func) {
        try {
            MYSQL.renderMetric(func, COL, KEYS);
            Assert.fail(func + " 返回数组，AggregationResult 没有对应语义，必须报错");
        } catch (AggregationNotSupportedException e) {
            Assert.assertTrue(e.getMessage().contains(func.name()));
        }
    }

    // ------------------------------------------------------------------
    // 方言选择
    // ------------------------------------------------------------------

    @Test
    public void dialectResolution() {
        Assert.assertSame(MySqlAggregationSqlBuilder.class,
                AggregationSqlBuilders.resolve(IEndTypeGetter.EndType.MySQL).getClass());
        Assert.assertSame(MySqlAggregationSqlBuilder.class,
                AggregationSqlBuilders.resolve(IEndTypeGetter.EndType.OceanBase).getClass());
        Assert.assertSame(MppAggregationSqlBuilder.class,
                AggregationSqlBuilders.resolve(IEndTypeGetter.EndType.Doris).getClass());
        Assert.assertSame(MppAggregationSqlBuilder.class,
                AggregationSqlBuilders.resolve(IEndTypeGetter.EndType.StarRocks).getClass());
    }

    @Test
    public void unsupportedDialectFailsWithSupportedList() {
        try {
            AggregationSqlBuilders.resolve(IEndTypeGetter.EndType.Postgres);
            Assert.fail("不支持的方言必须直接报错，不得回落内存聚合");
        } catch (AggregationNotSupportedException e) {
            Assert.assertTrue("错误信息应列出已支持类型：" + e.getMessage(),
                    e.getMessage().contains("starRocks") && e.getMessage().contains("doris"));
        }
    }

    private static GroupKey temporal(TimeGranularity granularity, int unitValue) {
        return GroupKey.temporal(COL, granularity, unitValue);
    }
}
