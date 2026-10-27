package com.qlangtech.tis.plugin.ontology.workshop.widget.impl.chartxy;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.ibatis.RowMap;
import com.qlangtech.tis.plugin.ontology.OntologyProperty;
import com.qlangtech.tis.plugin.ontology.OntologyType;
import com.qlangtech.tis.plugin.ontology.aggregation.AggregationCriteria;
import com.qlangtech.tis.plugin.ontology.impl.objtype.DefaultOntologyProperty;
import com.qlangtech.tis.plugin.ontology.workshop.model.AxisConfig;
import com.qlangtech.tis.plugin.ontology.workshop.model.CategoricalAxisConfig;
import com.qlangtech.tis.plugin.ontology.workshop.model.CategoricalBucketing;
import com.qlangtech.tis.plugin.ontology.workshop.model.ChartLayer;
import com.qlangtech.tis.plugin.ontology.workshop.model.SeriesConfig;
import com.qlangtech.tis.plugin.ontology.workshop.model.TemporalBucketing;
import com.qlangtech.tis.plugin.ontology.workshop.model.XAxisBucketing;
import com.qlangtech.tis.plugin.ontology.aggregation.TimeGranularity;
import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * {@link ChartXYDataProcessor#assemble} 的聚合结果后处理语义。
 *
 * <p>输入已经是数据库 GROUP BY 之后的分组行（列名固定为 {@code c0/c1/mN}），
 * 因此这些用例不碰数据库、不碰 TIS 运行期：只覆盖「分组行 → 图表 JSON」这一段 ——
 * 分桶还原、排序、百分比归一、桶数截断、分段展开。SQL 公式本身另见
 * {@code TestAggregationSqlBuilder}，分桶序号还原见 {@code TestTemporalBucketStart}。
 *
 * <p>这些语义在前端没有任何镜像实现 —— 算错了图表只是「看起来不对」，不会有任何报错。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/24
 */
public class TestChartXYAggregation {

    private static final String PROP_CATEGORY = "status";
    private static final String PROP_TIME = "created_at";
    private static final String PROP_VALUE = "amount";
    private static final String PROP_SEGMENT = "region";

    private static final List<OntologyProperty> COLS = List.of(
            prop(PROP_CATEGORY, OntologyType.STRING),
            prop(PROP_TIME, OntologyType.TIMESTAMP),
            prop(PROP_VALUE, OntologyType.DECIMAL),
            prop(PROP_SEGMENT, OntologyType.STRING));

    // ------------------------------------------------------------------
    // 聚合函数（数据库已经算好，这里只验证取值归一）
    // ------------------------------------------------------------------

    @Test
    public void countDoesNotNeedMetricProperty() {
        ChartLayer layer = layer(new CategoricalBucketing(), series("s", new CountAggregation()));
        JSONArray points = assemble(null, layer,
                group("OPEN", 2L), group("CLOSED", 1L));

        Assert.assertEquals(2L, pointsOf(points, "s", "OPEN").getJSONObject(0).getLongValue("y"));
        Assert.assertEquals(1L, pointsOf(points, "s", "CLOSED").getJSONObject(0).getLongValue("y"));
    }

    @Test
    public void numericAggregationsNormalizeToDouble() {
        assertMetric(new SumAggregation(), 60d);
        assertMetric(new AverageAggregation(), 20d);
        assertMetric(new MinAggregation(), 10d);
        assertMetric(new MaxAggregation(), 30d);
    }

    private static void assertMetric(AbstractSeriesAggregation agg, double expected) {
        ChartLayer layer = layer(new CategoricalBucketing(), series("s", prop(agg, PROP_VALUE)));
        JSONArray points = assemble(null, layer, group("A", expected));

        JSONArray bucket = pointsOf(points, "s", "A");
        Assert.assertEquals(1, bucket.size());
        JSONObject point = bucket.getJSONObject(0);
        // 无论驱动返回 Integer / Long / BigDecimal，最终都统一成 double
        Assert.assertTrue("y 应为 JSON 数字：" + point.get("y"), point.get("y") instanceof Number);
        Assert.assertEquals(expected, point.getDoubleValue("y"), 1e-9);
    }

    @Test
    public void countLikeMetricsNormalizeToLong() {
        ChartLayer layer = layer(new CategoricalBucketing(),
                series("s", prop(new ApproxUniqueCountAggregation(), PROP_VALUE)));
        // 驱动对 APPROX_COUNT_DISTINCT 返回的具体数值类型并不统一
        JSONArray points = assemble(null, layer, group("A", 2));

        Object y = pointsOf(points, "s", "A").getJSONObject(0).get("y");
        Assert.assertEquals(Long.class, y.getClass());
        Assert.assertEquals(2L, ((Long) y).longValue());
    }

    @Test
    public void nullMetricStaysNullNotZero() {
        ChartLayer layer = layer(new CategoricalBucketing(), series("s", prop(new SumAggregation(), PROP_VALUE)));
        JSONArray points = assemble(null, layer, group("A", null));

        // 全 NULL 的分组是「缺口」，不是 0；前端 nullDisplay 依赖这个区别
        Assert.assertNull(pointsOf(points, "s", "A").getJSONObject(0).get("y"));
    }

    @Test
    public void nonCountAggregationWithoutPropertyFailsLoudly() {
        ChartLayer layer = layer(new CategoricalBucketing(), series("s", new SumAggregation()));
        try {
            assemble(null, layer, group("A", 1L));
            Assert.fail("非 Count 聚合缺 property 时应抛可读错误，而不是静默算出 0");
        } catch (IllegalStateException e) {
            Assert.assertTrue("错误信息应点明是哪一层哪个系列：" + e.getMessage(),
                    e.getMessage().contains("s") && e.getMessage().contains("property"));
        }
    }

    // ------------------------------------------------------------------
    // 分桶
    // ------------------------------------------------------------------

    @Test
    public void categoricalBucketsByRawValue() {
        ChartLayer layer = layer(new CategoricalBucketing(), series("s", new CountAggregation()));
        JSONArray points = assemble(null, layer,
                group("A", 1L), group(1, 1L), group(true, 1L));

        // 分类键统一 String.valueOf：不这样做，1 与 "1" 会落在两个桶里
        assertKeys(points, "s", "1", "A", "true");
    }

    @Test
    public void temporalBucketIndexIsRestoredToBucketStart() {
        TemporalBucketing bucketing = new TemporalBucketing();
        bucketing.granularity = TimeGranularity.DAY;
        ChartLayer layer = layer(bucketing, series("s", new CountAggregation()));

        // SQL 返回的是「距 1970-01-01 的天数」，不是时间戳
        JSONArray points = assemble(null, layer,
                group(dayIndex(2026, 1, 1), 2L), group(dayIndex(2026, 1, 2), 1L));

        Assert.assertEquals("同一天的两行在库里已经是同一组", 2L,
                pointsOf(points, "s", "2026-01-01T00:00:00").getJSONObject(0).getLongValue("y"));
        Assert.assertEquals(1L,
                pointsOf(points, "s", "2026-01-02T00:00:00").getJSONObject(0).getLongValue("y"));
    }

    @Test
    public void temporalBucketAlignsByUnitValue() {
        // 每 3 个月一桶：一、二月落进同一桶（起点 2026-01），四月单独一桶
        TemporalBucketing bucketing = new TemporalBucketing();
        bucketing.granularity = TimeGranularity.MONTH;
        bucketing.unitValue = 3;
        ChartLayer layer = layer(bucketing, series("s", new CountAggregation()));

        // 数据库侧已经按 unitValue=3 对齐过，这里给的就是对齐后的序号
        JSONArray points = assemble(null, layer,
                group(monthIndex(2026, 1), 2L),
                group(monthIndex(2026, 4), 1L));

        Assert.assertEquals(2L,
                pointsOf(points, "s", "2026-01-01T00:00:00").getJSONObject(0).getLongValue("y"));
        Assert.assertEquals(1L,
                pointsOf(points, "s", "2026-04-01T00:00:00").getJSONObject(0).getLongValue("y"));
    }

    @Test
    public void nullGroupIsSkipped() {
        TemporalBucketing bucketing = new TemporalBucketing();
        bucketing.granularity = TimeGranularity.DAY;
        ChartLayer layer = layer(bucketing, series("s", new CountAggregation()));

        JSONArray points = assemble(null, layer,
                group(dayIndex(2026, 1, 1), 1L), group(null, 9L));

        // SQL 侧已经有 IS NOT NULL；这里再兜一次，保证不会画出 1970 的空桶
        Assert.assertEquals(1, points.getJSONObject(0).getJSONArray("points").size());
    }

    // ------------------------------------------------------------------
    // 分段 / 百分比
    // ------------------------------------------------------------------

    @Test
    public void segmentSplitsPointsWithinSameBucket() {
        SeriesConfig series = series("s", new CountAggregation());
        series.segmentBy = PROP_SEGMENT;
        ChartLayer layer = layer(new CategoricalBucketing(), series);

        JSONArray points = assemble(null, layer,
                segmentGroup("A", "north", 2L),
                segmentGroup("A", "south", 1L));

        Assert.assertEquals(2, pointsOf(points, "s", "A").size());
        Assert.assertEquals(2L, pointOf(points, "s", "A", "north").getLongValue("y"));
        Assert.assertEquals(1L, pointOf(points, "s", "A", "south").getLongValue("y"));
    }

    @Test
    public void percentageNormalizesAcrossSegmentsOfSameSeries() {
        SeriesConfig series = series("s", new CountAggregation());
        series.segmentBy = PROP_SEGMENT;
        series.segmentOverride = SeriesConfig.SegmentOverride.PERCENTAGE;
        ChartLayer layer = layer(new CategoricalBucketing(), series);

        JSONArray points = assemble(null, layer,
                segmentGroup("A", "north", 3L),
                segmentGroup("A", "south", 1L));

        Assert.assertEquals("3/4", 75d, pointOf(points, "s", "A", "north").getDoubleValue("y"), 1e-9);
        Assert.assertEquals("1/4", 25d, pointOf(points, "s", "A", "south").getDoubleValue("y"), 1e-9);
    }

    @Test
    public void percentageWithZeroTotalYieldsNullNotNaN() {
        SeriesConfig series = series("s", prop(new SumAggregation(), PROP_VALUE));
        series.segmentBy = PROP_SEGMENT;
        series.segmentOverride = SeriesConfig.SegmentOverride.PERCENTAGE;
        ChartLayer layer = layer(new CategoricalBucketing(), series);

        JSONArray points = assemble(null, layer, segmentGroup("A", "north", 0d));

        JSONObject north = pointOf(points, "s", "A", "north");
        // 分母为 0：输出 null（由前端 nullDisplay 决定怎么画），绝不能是 NaN ——
        // JSON 没有 NaN，序列化时会变成 null 或直接抛错，两种都不该由前端去猜
        Assert.assertNull("除数为 0 时应输出 null", north.get("y"));
    }

    @Test
    public void differentSegmentShapesDoNotPolluteEachOther() {
        SeriesConfig plain = series("plain", new CountAggregation());
        SeriesConfig segmented = series("segmented", new CountAggregation());
        segmented.segmentBy = PROP_SEGMENT;
        ChartLayer layer = layer(new CategoricalBucketing(), plain, segmented);

        List<AggregationPushdownPlanner.Shape> shapes = AggregationPushdownPlanner.plan(
                layer, layer.series, COLS, Collections.emptyList(), 1000);
        Assert.assertEquals("不同 segmentBy 必须拆成两条 SQL", 2, shapes.size());
        Assert.assertFalse(shapes.get(0).hasSegment());
        Assert.assertTrue(shapes.get(1).hasSegment());

        JSONArray points = ChartXYDataProcessor.assemble(null, layer, layer.series, List.of(
                new ChartXYDataProcessor.ShapeRows(shapes.get(0), List.of(group("A", 5L))),
                new ChartXYDataProcessor.ShapeRows(shapes.get(1), List.of(
                        segmentGroup("A", "north", 3L), segmentGroup("A", "south", 2L)))));

        Assert.assertEquals("不拆分的系列只有一个点", 5L,
                pointsOf(points, "plain", "A").getJSONObject(0).getLongValue("y"));
        Assert.assertEquals(3L,
                pointOf(points, "segmented", "A", "north").getLongValue("y"));
    }

    // ------------------------------------------------------------------
    // 排序 / 截断
    // ------------------------------------------------------------------

    @Test
    public void categoricalKeysSortByConfiguredOrder() {
        Assert.assertEquals("默认按键升序（A→Z）",
                Arrays.asList("A", "B", "C"),
                keysOf(sortBy(CategoricalAxisConfig.CategoricalSortBy.KEY_ASCENDING,
                        group("B", 1L), group("A", 1L), group("C", 1L)), "s"));
    }

    @Test
    public void categoricalValueSortUsesDrawnTotals() {
        // A 1 次、B 3 次、C 2 次
        Assert.assertEquals(Arrays.asList("B", "C", "A"), keysOf(
                sortBy(CategoricalAxisConfig.CategoricalSortBy.VALUE_DESCENDING,
                        group("A", 1L), group("B", 3L), group("C", 2L)), "s"));
        Assert.assertEquals(Arrays.asList("A", "C", "B"), keysOf(
                sortBy(CategoricalAxisConfig.CategoricalSortBy.VALUE_ASCENDING,
                        group("A", 1L), group("B", 3L), group("C", 2L)), "s"));
    }

    @Test
    public void temporalKeysAlwaysAscendRegardlessOfAxisSort() {
        TemporalBucketing bucketing = new TemporalBucketing();
        bucketing.granularity = TimeGranularity.DAY;
        ChartLayer layer = layer(bucketing, series("s", new CountAggregation()));

        JSONArray points = assemble(null, layer,
                group(dayIndex(2026, 3, 1), 1L), group(dayIndex(2026, 1, 1), 1L));

        Assert.assertEquals("时间桶必须按时间升序（乱序折线图会自我交叉）",
                Arrays.asList("2026-01-01T00:00:00", "2026-03-01T00:00:00"), keysOf(points, "s"));
    }

    @Test
    public void bucketLimitTruncatesAfterSorting() {
        CategoricalBucketing bucketing = new CategoricalBucketing();
        bucketing.limit = 2;
        ChartLayer layer = layer(bucketing, series("s", new CountAggregation()));

        JSONArray points = assemble(null, layer,
                group("C", 1L), group("A", 1L), group("B", 1L));

        // 先按键升序排好再截断，留下的是 A、B —— 而不是「前两行原始数据」对应的 C、A
        assertKeys(points, "s", "A", "B");
    }

    // ------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------

    private static JSONArray sortBy(CategoricalAxisConfig.CategoricalSortBy sortBy, RowMap... rows) {
        CategoricalAxisConfig xAxis = new CategoricalAxisConfig();
        xAxis.sortBy = sortBy;
        return assemble(xAxis, layer(new CategoricalBucketing(), series("s", new CountAggregation())), rows);
    }

    private static ChartLayer layer(XAxisBucketing bucketing, SeriesConfig... series) {
        ChartLayer layer = new ChartLayer();
        layer.title = "layer";
        layer.xAxisProperty = bucketing instanceof TemporalBucketing ? PROP_TIME : PROP_CATEGORY;
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

    /** 给聚合补上度量属性（Count 之外的聚合都要求它） */
    private static AbstractSeriesAggregation prop(AbstractSeriesAggregation agg, String property) {
        agg.property = property;
        return agg;
    }

    private static OntologyProperty prop(String name, OntologyType type) {
        return new DefaultOntologyProperty(name, false, true, "test", type);
    }

    /** 一个分组行：{@code c0} + {@code m0} */
    private static RowMap group(Object groupValue, Object metricValue) {
        RowMap row = new RowMap();
        row.put(AggregationCriteria.GROUP_ALIAS, groupValue);
        row.put(AggregationCriteria.metricAlias(0), metricValue);
        return row;
    }

    /** 一个带分段的分组行：{@code c0} + {@code c1} + {@code m0} */
    private static RowMap segmentGroup(Object groupValue, String segment, Object metricValue) {
        RowMap row = group(groupValue, metricValue);
        row.put(AggregationCriteria.SEGMENT_ALIAS, segment);
        return row;
    }

    /**
     * 走真实的 planner 得到 shape，再把「数据库已聚合的行」喂给 assemble。
     * 单 shape 场景；多 shape 见 {@link #differentSegmentShapesDoNotPolluteEachOther()}。
     */
    private static JSONArray assemble(AxisConfig xAxis, ChartLayer layer, RowMap... rows) {
        List<AggregationPushdownPlanner.Shape> shapes = AggregationPushdownPlanner.plan(
                layer, layer.series, COLS, Collections.emptyList(), 1000);
        List<ChartXYDataProcessor.ShapeRows> shapeRows = new ArrayList<>(shapes.size());
        for (AggregationPushdownPlanner.Shape shape : shapes) {
            shapeRows.add(new ChartXYDataProcessor.ShapeRows(shape, Arrays.asList(rows)));
        }
        return ChartXYDataProcessor.assemble(xAxis, layer, layer.series, shapeRows);
    }

    private static long dayIndex(int year, int month, int day) {
        return ChronoUnit.DAYS.between(LocalDate.of(1970, 1, 1), LocalDate.of(year, month, day));
    }

    private static long monthIndex(int year, int month) {
        return (long) year * 12 + (month - 1);
    }

    private static JSONArray pointsOf(JSONArray seriesArray, String seriesName, String x) {
        JSONArray points = seriesArrayOf(seriesArray, seriesName).getJSONArray("points");
        JSONArray result = new JSONArray();
        for (int i = 0; i < points.size(); i++) {
            JSONObject point = points.getJSONObject(i);
            if (x.equals(point.getString("x"))) {
                result.add(point);
            }
        }
        return result;
    }

    private static JSONObject pointOf(JSONArray seriesArray, String seriesName, String x, String segment) {
        for (Object o : pointsOf(seriesArray, seriesName, x)) {
            JSONObject point = (JSONObject) o;
            if (segment.equals(point.getString("segment"))) {
                return point;
            }
        }
        Assert.fail("找不到 x=" + x + " segment=" + segment + " 的数据点");
        return null;
    }

    private static JSONObject seriesArrayOf(JSONArray seriesArray, String seriesName) {
        for (Object o : seriesArray) {
            JSONObject series = (JSONObject) o;
            if (seriesName.equals(series.getString("name"))) {
                return series;
            }
        }
        Assert.fail("响应里没有系列 " + seriesName);
        return null;
    }

    private static void assertKeys(JSONArray seriesArray, String seriesName, String... expected) {
        Assert.assertEquals(Arrays.asList(expected), keysOf(seriesArray, seriesName));
    }

    /** 系列里各数据点的 x 序列（同一桶的多个 segment 会重复出现） */
    private static List<String> keysOf(JSONArray seriesArray, String seriesName) {
        JSONArray points = seriesArrayOf(seriesArray, seriesName).getJSONArray("points");
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < points.size(); i++) {
            keys.add(points.getJSONObject(i).getString("x"));
        }
        return keys;
    }
}
