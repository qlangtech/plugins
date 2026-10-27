package com.qlangtech.tis.plugin.ontology.workshop.widget.impl.chartxy;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.ibatis.RowMap;
import com.qlangtech.tis.plugin.ontology.AggregationFunc;
import com.qlangtech.tis.plugin.ontology.OntologyProperty;
import com.qlangtech.tis.plugin.ontology.aggregation.AggregationCriteria;
import com.qlangtech.tis.plugin.ontology.aggregation.AggregationNotSupportedException;
import com.qlangtech.tis.plugin.ontology.aggregation.AggregationResult;
import com.qlangtech.tis.plugin.ontology.aggregation.TimeGranularity;
import com.qlangtech.tis.plugin.ontology.impl.objtype.aggregation.AggregationSqlBuilders;
import com.qlangtech.tis.plugin.ontology.workshop.model.AxisConfig;
import com.qlangtech.tis.plugin.ontology.workshop.model.CategoricalAxisConfig;
import com.qlangtech.tis.plugin.ontology.workshop.model.ChartLayer;
import com.qlangtech.tis.plugin.ontology.workshop.model.SeriesConfig;
import com.qlangtech.tis.plugin.ontology.workshop.model.TemporalBucketing;
import com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopVariable;
import com.qlangtech.tis.plugin.ontology.workshop.model.XAxisBucketing;
import com.qlangtech.tis.plugin.ontology.workshop.model.variable.ObjectSetVariable;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.ChartXYWidget;
import org.apache.commons.lang3.StringUtils;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * ChartXY 的聚合执行器 —— 「数据在哪算」的答案。
 *
 * <h3>聚合一律下推到数据库</h3>
 * 图表需要的是「按 X 分桶 + 每个 series 一个度量」的聚合结果，而不是明细行。把明细拉回 JVM
 * 再累加，在小库上看不出问题，但 Doris / StarRocks 这类 MPP 上单表动辄上亿行：网络传输、
 * JVM 堆与单机计算都会先于数据库成为瓶颈，同时也完全浪费了 MPP 的并行聚合能力。
 *
 * <p>因此本类只负责「把图层翻译成 {@link AggregationCriteria}」，真正的扫描、分组与聚合由
 * {@link ObjectSetVariable#executeAggregation} → {@code DataSourceBinding#executeAggregation}
 * 在数据库端执行一条 {@code SELECT ... GROUP BY ... LIMIT}。JVM 侧只拿到至多
 * {@link #MAX_AGGREGATED_GROUPS} 个分组行，再做排序、百分比归一、桶数截断与 JSON 组装。
 *
 * <h3>为什么一个图层可能发多条 SQL</h3>
 * 同层内不同 series 的 {@code segmentBy} 可能不同（甚至为空），一条 SQL 只有一组 GROUP BY，
 * 无法同时满足两种形状。详见 {@link AggregationPushdownPlanner}。
 *
 * <h3>方言限制</h3>
 * 只有 MySQL / MariaDB / TiDB / OceanBase / Doris / StarRocks 实现了聚合 SQL 渲染；
 * 其余数据源直接报错，<b>不回落内存聚合</b> —— 静默回落会让大表在 MPP 上表现得「能跑但很慢」，
 * 掩盖问题。
 *
 * <h3>响应契约</h3>
 * <pre>
 * {
 *   "layers": [
 *     {
 *       "title": "图层标题",
 *       "type": "bar" | "line" | "scatter",
 *       "truncated": false,
 *       "series": [
 *         {"name": "系列名", "points": [{"x": "2026-10-01T00:00:00", "y": 12, "segment": null}]}
 *       ]
 *     }
 *   ]
 * }
 * </pre>
 * {@code truncated=true} 表示<b>分组数</b>超过上限、只返回了前 {@link #MAX_AGGREGATED_GROUPS} 组，
 * 前端应显式提示 —— 静默截断会让图表看起来「数据就这么点」。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/24
 */
public final class ChartXYDataProcessor {

    /**
     * 单图层聚合结果的<b>分组数</b>上限（不是源行数上限）。
     *
     * <p>取值直接复用核心 API 的默认值，避免两处各写一个数字。50000 组远大于图表实际能画的
     * 桶数（{@link XAxisBucketing#MAX_LIMIT}），同时把内存 / 传输量稳稳压在数 MB 量级。
     */
    public static final int MAX_AGGREGATED_GROUPS = AggregationCriteria.DEFAULT_MAX_GROUPS;

    /** 按天分桶的对齐基准（只用于还原桶起点）。 */
    private static final LocalDateTime DAY_EPOCH = LocalDate.of(1970, 1, 1).atStartOfDay();

    /** 按周分桶的对齐基准，取 1970-01-05（周一），与 SQL 侧的 {@code DATEDIFF(c, '1970-01-05')} 一致。 */
    private static final LocalDateTime WEEK_EPOCH = LocalDate.of(1970, 1, 5).atStartOfDay();

    private static final DateTimeFormatter TIME_LABEL = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");

    private ChartXYDataProcessor() {
    }

    /**
     * 处理整个 widget：逐图层取数并聚合。
     *
     * @param widget         表单投影还原出的 widget 实例
     * @param filtersByLayer 运行时过滤条件，key 是图层标题（{@link ChartLayer#identityValue()}），
     *                       value 是 {@code [{property, operator, value}]} 数组。可以是空对象，
     *                       但不能为 null（调用方负责兜底）
     */
    public static JSONObject process(ChartXYWidget widget, JSONObject filtersByLayer) {
        Objects.requireNonNull(widget, "widget can not be null");
        // 保存期已由 ChartXYWidget.DescriptorImpl#verify 拦截；这里兜住历史落盘数据与绕过表单的直接调用。
        // 轴性质必须查：X 配成连续轴时渲染器照样出图，只是分桶标签落进数值刻度 —— 不报错但空白
        String axisKindViolation = widget.axisKindViolation();
        if (axisKindViolation != null) {
            throw new IllegalStateException(axisKindViolation);
        }
        String orientationViolation = widget.orientationViolation();
        if (orientationViolation != null) {
            throw new IllegalStateException(orientationViolation);
        }
        JSONObject result = new JSONObject();
        JSONArray layers = new JSONArray();
        if (widget.layers != null) {
            for (ChartLayer layer : widget.layers) {
                layers.add(processLayer(widget, layer, filtersByLayer));
            }
        }
        result.put("layers", layers);
        return result;
    }

    private static JSONObject processLayer(ChartXYWidget widget, ChartLayer layer, JSONObject filtersByLayer) {
        if (layer == null) {
            throw new IllegalStateException("ChartXY layer can not be null");
        }
        JSONObject result = new JSONObject();
        result.put("title", layer.title);
        result.put("type", resolveLayerTypeToken(layer.layerType));
        result.put("truncated", false);
        result.put("series", new JSONArray());

        List<SeriesConfig> seriesList = layer.series == null ? Collections.emptyList() : layer.series;
        if (seriesList.isEmpty()) {
            return result;
        }
        if (StringUtils.isBlank(layer.xAxisProperty)) {
            throw new IllegalStateException("ChartXY layer '" + layer.title + "' xAxisProperty can not be blank");
        }
        if (layer.xAxisBucketing == null) {
            throw new IllegalStateException("ChartXY layer '" + layer.title + "' xAxisBucketing can not be null");
        }

        ObjectSetVariable objSetVar = resolveObjectSetVariable(layer);
        List<OntologyProperty> colsMeta = objSetVar.getColsMeta();
        List<AggregationPushdownPlanner.Filter> filters = resolveFilters(layer, filtersByLayer);
        List<AggregationPushdownPlanner.Shape> shapes = AggregationPushdownPlanner.plan(
                layer, seriesList, colsMeta, filters, MAX_AGGREGATED_GROUPS);

        boolean truncated = false;
        List<ShapeRows> shapeRows = new ArrayList<>(shapes.size());
        for (AggregationPushdownPlanner.Shape shape : shapes) {
            AggregationResult aggregationResult = executeAggregation(objSetVar, layer, shape.criteria());
            truncated |= aggregationResult.truncated();
            shapeRows.add(new ShapeRows(shape, aggregationResult.rows()));
        }
        result.put("truncated", truncated);
        result.put("series", assemble(widget.xAxis, layer, seriesList, shapeRows));
        return result;
    }

    /** 一个分组形状的聚合结果。 */
    record ShapeRows(AggregationPushdownPlanner.Shape shape, List<RowMap> rows) {
    }

    /**
     * 聚合结果 → 图层 series JSON（排序、百分比归一、桶数截断、分段展开）。
     *
     * <p>包级可见<b>为了可测</b>：外层需要活的 {@code ObjectSetVariable}，而这一段的全部分支
     * 都能用手工构造的 {@link RowMap}（列名就是 {@code c0/c1/mN}）覆盖，不依赖数据库。
     * 单测见 {@code TestChartXYAggregation}。
     */
    static JSONArray assemble(AxisConfig xAxis, ChartLayer layer, List<SeriesConfig> seriesList,
                              List<ShapeRows> shapeRows) {
        // 每个 series 一份「x 桶 -> (segment -> 聚合值)」，按 series 原始下标存放
        List<Map<XKey, Map<String, Object>>> allSeriesValues = new ArrayList<>(seriesList.size());
        for (int i = 0; i < seriesList.size(); i++) {
            allSeriesValues.add(new LinkedHashMap<>());
        }
        // x 桶的首次出现顺序，仅用于稳定遍历；真正顺序由 orderKeys 决定
        Map<XKey, XKey> allKeys = new LinkedHashMap<>();
        // 分类轴按值排序时的依据：同一 x 上所有 series 的绘制值之和（百分比归一之前）
        Map<XKey, Double> xTotals = new HashMap<>();

        for (ShapeRows shapeRow : shapeRows) {
            AggregationPushdownPlanner.Shape shape = shapeRow.shape();
            for (AggregationPushdownPlanner.SeriesPlan plan : shape.seriesPlans()) {
                Map<XKey, Map<String, Object>> seriesValues =
                        toSeriesValues(layer, shape, plan, shapeRow.rows());
                allSeriesValues.set(plan.seriesIndex(), seriesValues);
                for (Map.Entry<XKey, Map<String, Object>> entry : seriesValues.entrySet()) {
                    allKeys.putIfAbsent(entry.getKey(), entry.getKey());
                    xTotals.merge(entry.getKey(), totalOf(entry.getValue()), Double::sum);
                }
                if (seriesList.get(plan.seriesIndex()).segmentOverride == SeriesConfig.SegmentOverride.PERCENTAGE) {
                    applyPercentage(seriesValues);
                }
            }
        }

        List<XKey> orderedKeys = orderKeys(new ArrayList<>(allKeys.keySet()), xAxis, xTotals);
        int limit = resolveLimit(layer.xAxisBucketing);
        if (orderedKeys.size() > limit) {
            orderedKeys = orderedKeys.subList(0, limit);
        }
        return toJSON(seriesList, allSeriesValues, orderedKeys);
    }

    private static AggregationResult executeAggregation(ObjectSetVariable objSetVar, ChartLayer layer,
                                                        AggregationCriteria criteria) {
        try {
            return objSetVar.executeAggregation(criteria);
        } catch (AggregationNotSupportedException e) {
            // 只捕获这一个类型：宽泛的 UnsupportedOperationException 会把实现 bug 误报成「方言不支持」
            throw new IllegalStateException("ChartXY layer '" + layer.title
                    + "' does not support database side aggregation, supported data source types:"
                    + AggregationSqlBuilders.supportedTypes() + ", detail:" + e.getMessage(), e);
        }
    }

    // ------------------------------------------------------------------
    // 取数
    // ------------------------------------------------------------------

    private static ObjectSetVariable resolveObjectSetVariable(ChartLayer layer) {
        if (StringUtils.isBlank(layer.dataSourceVar)) {
            throw new IllegalStateException("ChartXY layer '" + layer.title + "' dataSourceVar can not be blank");
        }
        WorkshopVariable variable = WorkshopVariable.loadDetail(layer.dataSourceVar);
        if (!(variable instanceof ObjectSetVariable objSetVar)) {
            throw new IllegalStateException("ChartXY layer '" + layer.title + "' dataSourceVar:'"
                    + layer.dataSourceVar + "' is not an object set variable");
        }
        return objSetVar;
    }

    /**
     * 解析运行时过滤条件。列名合法性与「列是否存在」由 {@link AggregationPushdownPlanner} 统一校验。
     */
    private static List<AggregationPushdownPlanner.Filter> resolveFilters(ChartLayer layer, JSONObject filtersByLayer) {
        JSONArray filters = filtersByLayer == null ? null : filtersByLayer.getJSONArray(layer.title);
        if (filters == null || filters.isEmpty()) {
            return Collections.emptyList();
        }
        List<AggregationPushdownPlanner.Filter> result = new ArrayList<>(filters.size());
        for (int i = 0; i < filters.size(); i++) {
            JSONObject filter = filters.getJSONObject(i);
            String property = filter.getString("property");
            if (StringUtils.isBlank(property)) {
                throw new IllegalStateException("ChartXY layer '" + layer.title + "' filter property can not be blank");
            }
            result.add(new AggregationPushdownPlanner.Filter(
                    property, filter.getString("operator"), filter.get("value")));
        }
        return result;
    }

    // ------------------------------------------------------------------
    // 聚合结果 → 每个 series 的 (x 桶 -> (segment -> 值))
    // ------------------------------------------------------------------

    private static Map<XKey, Map<String, Object>> toSeriesValues(ChartLayer layer,
                                                                AggregationPushdownPlanner.Shape shape,
                                                                AggregationPushdownPlanner.SeriesPlan plan,
                                                                List<RowMap> rows) {
        final String metricAlias = AggregationCriteria.metricAlias(plan.metricSlot());
        Map<XKey, Map<String, Object>> seriesValues = new LinkedHashMap<>();
        for (RowMap row : rows) {
            XKey xKey = resolveXKey(layer, row.get(AggregationCriteria.GROUP_ALIAS));
            if (xKey == null) {
                continue;
            }
            String segment = shape.hasSegment()
                    ? stringify(row.get(AggregationCriteria.SEGMENT_ALIAS)) : null;
            seriesValues.computeIfAbsent(xKey, k -> new LinkedHashMap<>())
                    .put(segment, normalizeMetric(plan.func(), row.get(metricAlias)));
        }
        return seriesValues;
    }

    /**
     * 分桶键。{@code label} 是输出到前端的 x 值，同时是相等判据；{@code time} 非 null 时
     * 表示这是时间桶，按它升序排。
     */
    private record XKey(String label, LocalDateTime time) {
    }

    /**
     * 聚合结果的分组值 → X 桶。
     *
     * <p>时间分桶时 SQL 返回的是<b>对齐后的整数序号</b>，不是时间戳；这里用
     * {@link #bucketStart} 还原桶起点。绝不能把序号交给「按 epoch 毫秒解析」的逻辑，
     * 否则 20000 天会被解释成 1970-01-01 00:00:20。
     */
    private static XKey resolveXKey(ChartLayer layer, Object rawGroup) {
        if (rawGroup == null) {
            return null;
        }
        if (layer.xAxisBucketing instanceof TemporalBucketing temporal) {
            TimeGranularity granularity = temporal.granularity == null ? TimeGranularity.DAY : temporal.granularity;
            int unitValue = temporal.unitValue == null || temporal.unitValue < 1 ? 1 : temporal.unitValue;
            long alignedIndex = Math.floorDiv(((Number) rawGroup).longValue(), unitValue) * unitValue;
            LocalDateTime bucket = bucketStart(granularity, alignedIndex);
            return new XKey(bucket.format(TIME_LABEL), bucket);
        }
        return new XKey(stringify(rawGroup), null);
    }

    /**
     * 由 SQL 返回的桶序号还原桶起点。
     *
     * <p>序号公式见 {@code MySqlAggregationSqlBuilder#renderGroupKey}：SQL 只返回
     * {@code FLOOR(index/unit)*unit}，本方法负责把它翻译回日期。两边必须成对修改。
     *
     * <p>包级可见<b>为了可测</b>：这是 SQL 与 Java 之间唯一的语义接缝，
     * 单测用黄金日期断言，不依赖旧的截断算法。
     */
    static LocalDateTime bucketStart(TimeGranularity granularity, long index) {
        return switch (granularity) {
            case YEAR -> LocalDateTime.of(Math.toIntExact(index), 1, 1, 0, 0);
            case QUARTER -> LocalDateTime.of(Math.toIntExact(Math.floorDiv(index, 4)),
                    Math.toIntExact(Math.floorMod(index, 4)) * 3 + 1, 1, 0, 0);
            case MONTH -> LocalDateTime.of(Math.toIntExact(Math.floorDiv(index, 12)),
                    Math.toIntExact(Math.floorMod(index, 12)) + 1, 1, 0, 0);
            case WEEK -> WEEK_EPOCH.plusWeeks(index);
            case DAY -> DAY_EPOCH.plusDays(index);
            case HOUR -> DAY_EPOCH.plusHours(index);
            case MINUTE -> DAY_EPOCH.plusMinutes(index);
            case SECOND -> DAY_EPOCH.plusSeconds(index);
        };
    }

    /**
     * 把驱动返回的聚合值归一成前端能直接绘制的数字。
     *
     * <ul>
     *   <li>{@code null} 原样保留 —— 全 NULL 的分组是「缺口」而不是 0；</li>
     *   <li>计数类（COUNT / COUNT_DISTINCT / APPROX_CARDINALITY）统一 {@code Long}；</li>
     *   <li>其余（SUM / AVG / MIN / MAX）统一 {@code Double}，与旧内存实现的数值类型一致
     *       （Doris 的 AVG 可能返回 DECIMAL，{@code Number.doubleValue()} 一并覆盖）。</li>
     * </ul>
     */
    static Object normalizeMetric(AggregationFunc func, Object raw) {
        if (raw == null) {
            return null;
        }
        if (!(raw instanceof Number number)) {
            throw new IllegalStateException("unexpected aggregation result type:" + raw.getClass().getName()
                    + " for func:" + func);
        }
        return switch (func) {
            case COUNT, COUNT_DISTINCT, APPROX_CARDINALITY -> number.longValue();
            case SUM, AVG, MIN, MAX -> number.doubleValue();
            case COLLECT_LIST, COLLECT_SET -> throw new IllegalStateException(
                    "aggregation func:" + func + " is not supported by ChartXY");
        };
    }

    // ------------------------------------------------------------------
    // 排序 / 百分比 / 输出
    // ------------------------------------------------------------------

    private static List<XKey> orderKeys(List<XKey> keys, AxisConfig xAxis, Map<XKey, Double> xTotals) {
        boolean temporal = keys.stream().anyMatch(k -> k.time() != null);
        if (temporal) {
            keys.sort(Comparator.comparing(XKey::time));
            return keys;
        }
        CategoricalAxisConfig.CategoricalSortBy sortBy = resolveSortBy(xAxis);
        switch (sortBy) {
            // 排序键相同时按 label 字典序兜底：多个 shape 的返回顺序不保证一致，
            // 没有 tie-break 时同值桶的相对顺序会随查询结果抖动。
            case VALUE_ASCENDING -> keys.sort(Comparator
                    .comparingDouble((XKey k) -> xTotals.getOrDefault(k, 0d))
                    .thenComparing(XKey::label));
            case VALUE_DESCENDING -> keys.sort(Comparator
                    .comparingDouble((XKey k) -> xTotals.getOrDefault(k, 0d)).reversed()
                    .thenComparing(XKey::label));
            case KEY_ASCENDING -> keys.sort(Comparator.comparing(XKey::label));
            default -> throw new IllegalStateException("illegal categorical sortBy:" + sortBy);
        }
        return keys;
    }

    private static CategoricalAxisConfig.CategoricalSortBy resolveSortBy(AxisConfig xAxis) {
        if (xAxis instanceof CategoricalAxisConfig categorical && categorical.sortBy != null) {
            return categorical.sortBy;
        }
        return CategoricalAxisConfig.CategoricalSortBy.KEY_ASCENDING;
    }

    private static int resolveLimit(XAxisBucketing bucketing) {
        Integer limit = bucketing.limit;
        if (limit == null || limit < 1) {
            return XAxisBucketing.MAX_LIMIT;
        }
        return Math.min(limit, XAxisBucketing.MAX_LIMIT);
    }

    private static JSONArray toJSON(List<SeriesConfig> seriesList,
                                    List<Map<XKey, Map<String, Object>>> allSeriesValues,
                                    List<XKey> orderedKeys) {
        JSONArray result = new JSONArray();
        for (int i = 0; i < seriesList.size(); i++) {
            SeriesConfig series = seriesList.get(i);
            Map<XKey, Map<String, Object>> seriesValues = allSeriesValues.get(i);
            JSONArray points = new JSONArray();
            for (XKey key : orderedKeys) {
                Map<String, Object> segmentValues = seriesValues.get(key);
                if (segmentValues == null) {
                    continue;
                }
                List<String> segments = new ArrayList<>(segmentValues.keySet());
                segments.sort(Comparator.nullsFirst(Comparator.naturalOrder()));
                for (String segment : segments) {
                    JSONObject point = new JSONObject();
                    point.put("x", key.label());
                    point.put("y", segmentValues.get(segment));
                    point.put("segment", segment);
                    points.add(point);
                }
            }
            JSONObject seriesJson = new JSONObject();
            seriesJson.put("name", series.name);
            seriesJson.put("points", points);
            result.add(seriesJson);
        }
        return result;
    }

    /**
     * 百分比分段：同一 series 的同一 x 上跨 segment 归一化。
     *
     * <p>除数为 0 时输出 {@code null} 而不是 {@code NaN} —— JSON 没有 NaN，
     * fastjson 会把它写成 null 或抛错，取决于版本，都不该由前端去猜。
     */
    private static void applyPercentage(Map<XKey, Map<String, Object>> seriesValues) {
        for (Map<String, Object> segmentValues : seriesValues.values()) {
            double sum = 0d;
            boolean hasValue = false;
            for (Object value : segmentValues.values()) {
                if (value instanceof Number number) {
                    sum += number.doubleValue();
                    hasValue = true;
                }
            }
            if (!hasValue || sum == 0d) {
                for (Map.Entry<String, Object> entry : segmentValues.entrySet()) {
                    entry.setValue(null);
                }
                continue;
            }
            for (Map.Entry<String, Object> entry : segmentValues.entrySet()) {
                if (entry.getValue() instanceof Number number) {
                    entry.setValue(number.doubleValue() / sum * 100d);
                } else {
                    entry.setValue(null);
                }
            }
        }
    }

    private static double totalOf(Map<String, Object> segmentValues) {
        double total = 0d;
        for (Object value : segmentValues.values()) {
            if (value instanceof Number number) {
                total += number.doubleValue();
            }
        }
        return total;
    }

    private static String stringify(Object value) {
        return value == null ? null : String.valueOf(value);
    }

    /** 图层画法到前端 Chart.js dataset type 的 token。 */
    private static String resolveLayerTypeToken(AbstractLayerType layerType) {
        if (layerType instanceof LineLayerType) {
            return "line";
        }
        if (layerType instanceof ScatterLayerType) {
            return "scatter";
        }
        // 未配置/未知画法按柱状图处理：BarLayerType 是表单默认项，也是这里最保守的兜底
        return "bar";
    }
}
