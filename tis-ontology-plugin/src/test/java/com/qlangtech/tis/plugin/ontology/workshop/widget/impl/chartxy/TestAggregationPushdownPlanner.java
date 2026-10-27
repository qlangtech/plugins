package com.qlangtech.tis.plugin.ontology.workshop.widget.impl.chartxy;

import com.qlangtech.tis.plugin.ontology.AggregationFunc;
import com.qlangtech.tis.plugin.ontology.OntologyProperty;
import com.qlangtech.tis.plugin.ontology.OntologyType;
import com.qlangtech.tis.plugin.ontology.aggregation.AggregationCriteria;
import com.qlangtech.tis.plugin.ontology.aggregation.GroupKey;
import com.qlangtech.tis.plugin.ontology.aggregation.TimeGranularity;
import com.qlangtech.tis.plugin.ontology.impl.objtype.DefaultOntologyProperty;
import com.qlangtech.tis.plugin.ontology.workshop.model.CategoricalBucketing;
import com.qlangtech.tis.plugin.ontology.workshop.model.ChartLayer;
import com.qlangtech.tis.plugin.ontology.workshop.model.SeriesConfig;
import com.qlangtech.tis.plugin.ontology.workshop.model.TemporalBucketing;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * {@link AggregationPushdownPlanner} 的翻译与类型闸门。
 *
 * <p>纯函数：列元数据由手工构造的 {@link OntologyProperty} 提供，不连数据库。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/24
 */
public class TestAggregationPushdownPlanner {

    private static final String PROP_CATEGORY = "status";
    private static final String PROP_TIME = "created_at";
    private static final String PROP_DATE = "created_date";
    private static final String PROP_VALUE = "amount";
    private static final String PROP_NAME = "name";
    private static final String PROP_SEGMENT = "region";

    private static final List<OntologyProperty> COLS = List.of(
            prop(PROP_CATEGORY, OntologyType.STRING),
            prop(PROP_TIME, OntologyType.TIMESTAMP),
            prop(PROP_DATE, OntologyType.DATE),
            prop(PROP_VALUE, OntologyType.DECIMAL),
            prop(PROP_NAME, OntologyType.STRING),
            prop(PROP_SEGMENT, OntologyType.STRING));

    // ------------------------------------------------------------------
    // 分组键
    // ------------------------------------------------------------------

    @Test
    public void categoricalXBecomesRawGroupKey() {
        ChartLayer layer = layer(new CategoricalBucketing(), series("s", new CountAggregation()));
        List<AggregationPushdownPlanner.Shape> shapes = plan(layer, noFilters());

        Assert.assertEquals(1, shapes.size());
        AggregationCriteria criteria = shapes.get(0).criteria();
        Assert.assertEquals(1, criteria.getGroupKeys().size());
        GroupKey key = criteria.getGroupKeys().get(0);
        Assert.assertEquals(PROP_CATEGORY, key.column());
        Assert.assertFalse("分类分桶不应带时间粒度", key.isTemporal());
        Assert.assertEquals("COUNT 不需要度量列", 1, criteria.getMetrics().size());
        Assert.assertEquals(AggregationFunc.COUNT, criteria.getMetrics().get(0).func());
        Assert.assertNull(criteria.getMetrics().get(0).property());
    }

    @Test
    public void temporalXCarriesGranularityAndUnit() {
        TemporalBucketing bucketing = new TemporalBucketing();
        bucketing.granularity = TimeGranularity.DAY;
        bucketing.unitValue = 7;
        ChartLayer layer = layer(PROP_TIME, bucketing, series("s", new CountAggregation()));

        GroupKey key = plan(layer, noFilters()).get(0).criteria().getGroupKeys().get(0);
        Assert.assertTrue(key.isTemporal());
        Assert.assertEquals(TimeGranularity.DAY, key.granularity());
        Assert.assertEquals("unitValue 必须原样下推给 SQL 公式", 7, key.unitValue());
    }

    @Test
    public void segmentByAddsSecondGroupKey() {
        SeriesConfig series = series("s", new CountAggregation());
        series.segmentBy = PROP_SEGMENT;
        ChartLayer layer = layer(new CategoricalBucketing(), series);

        AggregationCriteria criteria = plan(layer, noFilters()).get(0).criteria();
        Assert.assertEquals(2, criteria.getGroupKeys().size());
        Assert.assertEquals("segment 固定占 c1 别名", AggregationCriteria.SEGMENT_ALIAS,
                AggregationCriteria.groupAlias(1));
        Assert.assertEquals(PROP_SEGMENT, criteria.getGroupKeys().get(1).column());
        Assert.assertTrue("segment 列必须在 targetCols 白名单里",
                criteria.getTargetCols().contains(PROP_SEGMENT));
    }

    // ------------------------------------------------------------------
    // 形状拆分 / 指标去重
    // ------------------------------------------------------------------

    @Test
    public void differentSegmentBySplitsIntoSeparateShapes() {
        SeriesConfig plain = series("plain", new CountAggregation());
        SeriesConfig segmented = series("segmented", new CountAggregation());
        segmented.segmentBy = PROP_SEGMENT;
        ChartLayer layer = layer(new CategoricalBucketing(), plain, segmented);

        List<AggregationPushdownPlanner.Shape> shapes = plan(layer, noFilters());
        Assert.assertEquals("一条 SQL 只有一组 GROUP BY，形状不同必须拆开", 2, shapes.size());
        Assert.assertEquals("", shapes.get(0).segmentBy());
        Assert.assertEquals(PROP_SEGMENT, shapes.get(1).segmentBy());
        Assert.assertEquals("series 下标必须指回原始列表", 1, shapes.get(1).seriesPlans().get(0).seriesIndex());
    }

    @Test
    public void identicalMetricsShareOneSqlSlot() {
        ChartLayer layer = layer(new CategoricalBucketing(),
                series("a", new CountAggregation()), series("b", new CountAggregation()));

        AggregationCriteria criteria = plan(layer, noFilters()).get(0).criteria();
        Assert.assertEquals("相同 (func, property) 只生成一个聚合表达式", 1, criteria.getMetrics().size());
        Assert.assertEquals(0, criteria.metricIndex(AggregationFunc.COUNT, null));
    }

    @Test
    public void sumMetricRequiresNumericProperty() {
        assertFails(new SumAggregation(), PROP_NAME, OntologyType.STRING, "numeric");
        assertFails(new AverageAggregation(), PROP_NAME, OntologyType.STRING, "numeric");
        assertFails(new MinAggregation(), PROP_NAME, OntologyType.STRING, "numeric");
        assertFails(new MaxAggregation(), PROP_NAME, OntologyType.STRING, "numeric");
    }

    private static void assertFails(AbstractSeriesAggregation aggregation, String property, OntologyType type,
                                    String expectedMessagePart) {
        SeriesConfig series = series("s", prop(aggregation, property));
        ChartLayer layer = layer(new CategoricalBucketing(), series);
        try {
            plan(layer, noFilters());
            Assert.fail(aggregation.getClass().getSimpleName() + " on " + type + " should be rejected");
        } catch (IllegalStateException e) {
            Assert.assertTrue("错误信息应说明原因：" + e.getMessage(),
                    e.getMessage().contains(expectedMessagePart));
        }
    }

    @Test
    public void numericMetricIsAccepted() {
        SeriesConfig series = series("s", prop(new SumAggregation(), PROP_VALUE));
        ChartLayer layer = layer(new CategoricalBucketing(), series);

        AggregationCriteria criteria = plan(layer, noFilters()).get(0).criteria();
        Assert.assertEquals(AggregationFunc.SUM, criteria.getMetrics().get(0).func());
        Assert.assertEquals(PROP_VALUE, criteria.getMetrics().get(0).property());
        Assert.assertTrue(criteria.getTargetCols().contains(PROP_VALUE));
    }

    // ------------------------------------------------------------------
    // 类型闸门
    // ------------------------------------------------------------------

    @Test
    public void temporalBucketingRejectsNonTimeColumn() {
        ChartLayer layer = layer(PROP_CATEGORY, dayBucketing(), series("s", new CountAggregation()));
        try {
            plan(layer, noFilters());
            Assert.fail("对字符串列做时间分桶应直接报错，而不是静默退化为分类分桶");
        } catch (IllegalStateException e) {
            Assert.assertTrue(e.getMessage().contains("temporal"));
        }
    }

    @Test
    public void dateColumnRejectsSubDayGranularity() {
        TemporalBucketing bucketing = new TemporalBucketing();
        bucketing.granularity = TimeGranularity.HOUR;
        ChartLayer layer = layer(PROP_DATE, bucketing, series("s", new CountAggregation()));
        try {
            plan(layer, noFilters());
            Assert.fail("DATE 没有时间部分，按小时切桶会把所有行塞进同一个桶");
        } catch (IllegalStateException e) {
            Assert.assertTrue(e.getMessage().contains("granularity"));
        }
    }

    @Test
    public void dateColumnAcceptsDayGranularity() {
        ChartLayer layer = layer(PROP_DATE, dayBucketing(), series("s", new CountAggregation()));
        Assert.assertEquals(1, plan(layer, noFilters()).size());
    }

    // ------------------------------------------------------------------
    // 列白名单
    // ------------------------------------------------------------------

    @Test
    public void unknownXColumnFails() {
        ChartLayer layer = new ChartLayer();
        layer.title = "layer";
        layer.xAxisProperty = "not_exists";
        layer.xAxisBucketing = new CategoricalBucketing();
        layer.series = new ArrayList<>(List.of(series("s", new CountAggregation())));
        try {
            plan(layer, noFilters());
            Assert.fail("列名必须命中本体属性，否则会拼出合法但不存在的列");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("not_exists"));
        }
    }

    @Test
    public void illegalColumnNameFailsRegex() {
        ChartLayer layer = new ChartLayer();
        layer.title = "layer";
        layer.xAxisProperty = "status`; drop table t; --";
        layer.xAxisBucketing = new CategoricalBucketing();
        layer.series = new ArrayList<>(List.of(series("s", new CountAggregation())));
        try {
            plan(layer, noFilters());
            Assert.fail("非法标识符必须在拼 SQL 之前被正则拒绝");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("legal db column name"));
        }
    }

    @Test
    public void filterColumnsAreValidatedAndApplied() {
        ChartLayer layer = layer(new CategoricalBucketing(), series("s", new CountAggregation()));
        List<AggregationPushdownPlanner.Filter> filters = List.of(
                new AggregationPushdownPlanner.Filter(PROP_SEGMENT, "equals", "north"));

        AggregationCriteria criteria = plan(layer, filters).get(0).criteria();
        Assert.assertEquals(1, criteria.getFilters().size());
        Assert.assertTrue(criteria.getTargetCols().contains(PROP_SEGMENT));
    }

    @Test
    public void unknownFilterColumnFails() {
        ChartLayer layer = layer(new CategoricalBucketing(), series("s", new CountAggregation()));
        try {
            plan(layer, List.of(new AggregationPushdownPlanner.Filter("not_exists", "equals", "x")));
            Assert.fail("过滤列同样必须命中本体属性");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("not_exists"));
        }
    }

    // ------------------------------------------------------------------
    // 聚合映射
    // ------------------------------------------------------------------

    @Test
    public void aggregationMapping() {
        Assert.assertEquals(AggregationFunc.COUNT, AggregationPushdownPlanner.toFunc(new CountAggregation()));
        Assert.assertEquals(AggregationFunc.SUM, AggregationPushdownPlanner.toFunc(new SumAggregation()));
        Assert.assertEquals(AggregationFunc.AVG, AggregationPushdownPlanner.toFunc(new AverageAggregation()));
        Assert.assertEquals(AggregationFunc.MIN, AggregationPushdownPlanner.toFunc(new MinAggregation()));
        Assert.assertEquals(AggregationFunc.MAX, AggregationPushdownPlanner.toFunc(new MaxAggregation()));
        Assert.assertEquals(AggregationFunc.APPROX_CARDINALITY,
                AggregationPushdownPlanner.toFunc(new ApproxUniqueCountAggregation()));
    }

    // ------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------

    private static List<AggregationPushdownPlanner.Shape> plan(
            ChartLayer layer, List<AggregationPushdownPlanner.Filter> filters) {
        return AggregationPushdownPlanner.plan(layer, layer.series, COLS, filters, 1000);
    }

    private static List<AggregationPushdownPlanner.Filter> noFilters() {
        return Collections.emptyList();
    }

    private static TemporalBucketing dayBucketing() {
        TemporalBucketing bucketing = new TemporalBucketing();
        bucketing.granularity = TimeGranularity.DAY;
        return bucketing;
    }

    private static ChartLayer layer(com.qlangtech.tis.plugin.ontology.workshop.model.XAxisBucketing bucketing,
                                    SeriesConfig... series) {
        return layer(PROP_CATEGORY, bucketing, series);
    }

    private static ChartLayer layer(String xProperty,
                                    com.qlangtech.tis.plugin.ontology.workshop.model.XAxisBucketing bucketing,
                                    SeriesConfig... series) {
        ChartLayer layer = new ChartLayer();
        layer.title = "layer";
        layer.xAxisProperty = xProperty;
        layer.xAxisBucketing = bucketing;
        layer.series = new ArrayList<>(Arrays.asList(series));
        return layer;
    }

    private static SeriesConfig series(String name, AbstractSeriesAggregation aggregation) {
        SeriesConfig series = new SeriesConfig();
        series.name = name;
        series.aggregation = aggregation;
        return series;
    }

    private static AbstractSeriesAggregation prop(AbstractSeriesAggregation agg, String property) {
        agg.property = property;
        return agg;
    }

    private static OntologyProperty prop(String name, OntologyType type) {
        return new DefaultOntologyProperty(name, false, true, "test", type);
    }
}
