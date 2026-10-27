package com.qlangtech.tis.plugin.ontology.workshop.widget.impl.chartxy;

import com.qlangtech.tis.plugin.ValidatorCommons;
import com.qlangtech.tis.plugin.ontology.AggregationFunc;
import com.qlangtech.tis.plugin.ontology.OntologyProperty;
import com.qlangtech.tis.plugin.ontology.OntologyType;
import com.qlangtech.tis.plugin.ontology.aggregation.AggregationCriteria;
import com.qlangtech.tis.plugin.ontology.aggregation.GroupKey;
import com.qlangtech.tis.plugin.ontology.aggregation.TimeGranularity;
import com.qlangtech.tis.plugin.ontology.workshop.model.ChartLayer;
import com.qlangtech.tis.plugin.ontology.workshop.model.CategoricalBucketing;
import com.qlangtech.tis.plugin.ontology.workshop.model.SeriesConfig;
import com.qlangtech.tis.plugin.ontology.workshop.model.TemporalBucketing;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 把「图表图层 + 一组系列」翻译成数据库端聚合查询条件。
 *
 * <h3>为什么一个图层可能拆成多条查询</h3>
 * 一条 SQL 只有一组 {@code GROUP BY}。同一图层里有的系列要按 {@code segmentBy} 拆、
 * 有的不拆，两者无法共用同一条语句：拆分会对不拆的系列产生「同一 X 出现多行」的错误结果。
 * 因此按 {@code segmentBy} 把系列分组，每个分组一条 SQL（通常 1~2 条）。
 *
 * <h3>类型闸门为什么在这里而不是 SQL 层</h3>
 * 列名与类型来自本体配置（{@code ObjectSetVariable.getColsMeta()}），是<b>不需要连数据库</b>
 * 就能拿到的事实。在拼 SQL 之前就把「对字符串求和」「对 DATE 按小时切桶」这类错误挡住，
 * 比让数据库报一条难以定位的语法/类型错误要好。
 *
 * <p>本类是纯函数：不碰数据源、不碰变量，单测直接用一组 {@link OntologyProperty} 驱动。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/24
 */
final class AggregationPushdownPlanner {

    /**
     * 可以参与 SUM / AVG / MIN / MAX 的类型。
     *
     * <p>MIN / MAX 也限制在数值：图表 {@code y} 必须是数字，时间型 MIN/MAX 没有合法的
     * {@code y} 编码（响应契约里 {@code null} 表示缺口，不表示日期）。
     */
    private static final Set<OntologyType> NUMERIC_TYPES = Set.of(
            OntologyType.INTEGER, OntologyType.SHORT, OntologyType.BYTE,
            OntologyType.LONG, OntologyType.FLOAT, OntologyType.DOUBLE, OntologyType.DECIMAL);

    /**
     * 不能做去重的类型：数组 / 向量 / 结构 / 地理 / 二进制既没有稳定的相等语义，
     * 各数据库的 DISTINCT 行为也不一致。
     */
    private static final Set<OntologyType> NON_DISTINCT_TYPES = Set.of(
            OntologyType.VECTOR, OntologyType.ARRAY, OntologyType.STRUCT,
            OntologyType.GEOPOINT, OntologyType.GEOSHAPE, OntologyType.MEDIA_REFERENCE);

    private AggregationPushdownPlanner() {
    }

    /**
     * 一条运行时过滤条件（来自 {@code get_data} 的 {@code filtersByLayer}）。
     */
    record Filter(String property, String operator, Object value) {
    }

    /**
     * 一个系列在聚合结果里的取数位置。
     *
     * @param metricSlot 指标别名槽位（相同 func+property 的系列共用一个槽位）
     * @param segment    本系列是否按 segment 拆分（= 所在 shape 是否带 segment 键）
     */
    record SeriesPlan(int seriesIndex, String seriesName, AggregationFunc func, int metricSlot, boolean segment) {
    }

    /**
     * 一个分组形状：一条 SQL + 该 SQL 服务的系列。
     */
    record Shape(String segmentBy, AggregationCriteria criteria, List<SeriesPlan> seriesPlans) {

        boolean hasSegment() {
            return !segmentBy.isEmpty();
        }
    }

    /**
     * @param colsMeta 本体属性（列名 + 类型），来自 {@code ObjectSetVariable#getColsMeta()}
     */
    static List<Shape> plan(ChartLayer layer, List<SeriesConfig> seriesList,
                            List<OntologyProperty> colsMeta, List<Filter> filters, int maxGroups) {
        Map<String, OntologyType> cols = indexCols(colsMeta);
        final String xCol = layer.xAxisProperty;
        final OntologyType xColType = requireCol(cols, xCol, "xAxisProperty");
        final TemporalBucketing temporal = layer.xAxisBucketing instanceof TemporalBucketing t ? t : null;
        if (temporal != null) {
            checkTemporalX(xCol, xColType, temporal);
        } else if (!(layer.xAxisBucketing instanceof CategoricalBucketing)) {
            throw new IllegalStateException("ChartXY layer '" + layer.title + "' illegal xAxisBucketing type:"
                    + (layer.xAxisBucketing == null ? "null" : layer.xAxisBucketing.getClass().getName()));
        }
        for (Filter filter : filters) {
            requireCol(cols, filter.property(), "filter property");
        }

        // 按 segmentBy 分形状；LinkedHashMap 保证 SQL 执行顺序与系列出现顺序一致
        Map<String, List<Integer>> shapeSeries = new LinkedHashMap<>();
        for (int i = 0; i < seriesList.size(); i++) {
            SeriesConfig series = seriesList.get(i);
            String segmentKey = StringUtils.isBlank(series.segmentBy) ? "" : series.segmentBy;
            shapeSeries.computeIfAbsent(segmentKey, k -> new ArrayList<>()).add(i);
        }

        List<Shape> shapes = new ArrayList<>(shapeSeries.size());
        for (Map.Entry<String, List<Integer>> entry : shapeSeries.entrySet()) {
            final String segmentBy = entry.getKey();
            AggregationCriteria criteria = new AggregationCriteria();
            criteria.addTargetCol(xCol);
            criteria.addGroupKey(temporal == null
                    ? GroupKey.rawColumn(xCol)
                    : GroupKey.temporal(xCol,
                    temporal.granularity == null ? TimeGranularity.DAY : temporal.granularity,
                    temporal.unitValue));
            if (!segmentBy.isEmpty()) {
                requireCol(cols, segmentBy, "segmentBy");
                criteria.addTargetCol(segmentBy);
                criteria.addGroupKey(GroupKey.rawColumn(segmentBy));
            }

            List<SeriesPlan> plans = new ArrayList<>(entry.getValue().size());
            for (Integer seriesIndex : entry.getValue()) {
                SeriesConfig series = seriesList.get(seriesIndex);
                AggregationFunc func = toFunc(resolveAggregation(series));
                String property = null;
                if (func != AggregationFunc.COUNT) {
                    property = series.aggregation.property;
                    if (StringUtils.isBlank(property)) {
                        throw new IllegalStateException("ChartXY layer '" + layer.title + "' series '" + series.name
                                + "' aggregation:" + series.aggregation.getClass().getSimpleName()
                                + " requires a metric property, but it is blank");
                    }
                    OntologyType metricType = requireCol(cols, property, "aggregation.property");
                    checkMetricType(series, func, metricType);
                    criteria.addTargetCol(property);
                }
                criteria.addMetric(func, property);
                plans.add(new SeriesPlan(seriesIndex, series.name, func,
                        criteria.metricIndex(func, property), !segmentBy.isEmpty()));
            }

            // ⚠️ 必须「先 addTargetCol 再 addFilter」，与 DataSourceQueryCriteria 的白名单边界一致
            for (Filter filter : filters) {
                criteria.addTargetCol(filter.property());
                criteria.addFilter(filter.property(), filter.operator(), filter.value());
            }
            criteria.setMaxGroups(maxGroups);
            shapes.add(new Shape(segmentBy, criteria, plans));
        }
        return shapes;
    }

    /**
     * 系列聚合方式 → 数据库聚合函数。
     */
    static AggregationFunc toFunc(AbstractSeriesAggregation aggregation) {
        if (aggregation instanceof CountAggregation) {
            return AggregationFunc.COUNT;
        }
        if (aggregation instanceof SumAggregation) {
            return AggregationFunc.SUM;
        }
        if (aggregation instanceof AverageAggregation) {
            return AggregationFunc.AVG;
        }
        if (aggregation instanceof MinAggregation) {
            return AggregationFunc.MIN;
        }
        if (aggregation instanceof MaxAggregation) {
            return AggregationFunc.MAX;
        }
        if (aggregation instanceof ApproxUniqueCountAggregation) {
            return AggregationFunc.APPROX_CARDINALITY;
        }
        throw new IllegalStateException("illegal aggregation type:" + aggregation.getClass().getName());
    }

    private static AbstractSeriesAggregation resolveAggregation(SeriesConfig series) {
        if (series.aggregation == null) {
            throw new IllegalStateException("ChartXY series '" + series.name + "' aggregation can not be null");
        }
        return series.aggregation;
    }

    private static Map<String, OntologyType> indexCols(List<OntologyProperty> colsMeta) {
        Map<String, OntologyType> cols = new LinkedHashMap<>();
        for (OntologyProperty prop : colsMeta) {
            cols.put(prop.getName(), prop.parseOntologyType());
        }
        return cols;
    }

    /**
     * 列必须同时满足「正则合法」与「本体里真的存在」。
     *
     * <p>正则挡住注入（转义标识符挡不住自带反引号的列名），本体白名单挡住「合法但不存在 /
     * 不属于这个对象」的列。
     */
    private static OntologyType requireCol(Map<String, OntologyType> cols, String col, String what) {
        if (StringUtils.isBlank(col)) {
            throw new IllegalArgumentException(what + " can not be blank");
        }
        if (!ValidatorCommons.PATTERN_DB_COL_NAME.matcher(col).matches()) {
            throw new IllegalArgumentException(what + " column:'" + col
                    + "' is not a legal db column name, must match " + ValidatorCommons.PATTERN_DB_COL_NAME.pattern());
        }
        OntologyType type = cols.get(col);
        if (type == null) {
            throw new IllegalArgumentException(what + " column:'" + col
                    + "' does not exist in object set properties:" + cols.keySet());
        }
        return type;
    }

    private static void checkTemporalX(String xCol, OntologyType xType, TemporalBucketing temporal) {
        if (xType != OntologyType.DATE && xType != OntologyType.TIMESTAMP) {
            throw new IllegalStateException("ChartXY x property:'" + xCol + "' type:" + xType
                    + " can not use temporal bucketing, only DATE/TIMESTAMP support it");
        }
        TimeGranularity granularity = temporal.granularity == null
                ? TimeGranularity.DAY : temporal.granularity;
        if (xType == OntologyType.DATE
                && (granularity == TimeGranularity.HOUR
                || granularity == TimeGranularity.MINUTE
                || granularity == TimeGranularity.SECOND)) {
            throw new IllegalStateException("ChartXY x property:'" + xCol
                    + "' is a DATE (no time part), granularity:" + granularity
                    + " would put every row into a single bucket; use TIMESTAMP or a coarser granularity");
        }
    }

    private static void checkMetricType(SeriesConfig series, AggregationFunc func, OntologyType metricType) {
        switch (func) {
            case SUM, AVG, MIN, MAX -> {
                if (!NUMERIC_TYPES.contains(metricType)) {
                    throw new IllegalStateException("ChartXY series '" + series.name + "' aggregation:" + func
                            + " requires a numeric property, but property:'" + series.aggregation.property
                            + "' type is:" + metricType);
                }
            }
            case COUNT_DISTINCT, APPROX_CARDINALITY -> {
                if (NON_DISTINCT_TYPES.contains(metricType)) {
                    throw new IllegalStateException("ChartXY series '" + series.name + "' aggregation:" + func
                            + " does not support property type:" + metricType);
                }
            }
            default -> {
                // COUNT 没有度量列
            }
        }
    }
}
