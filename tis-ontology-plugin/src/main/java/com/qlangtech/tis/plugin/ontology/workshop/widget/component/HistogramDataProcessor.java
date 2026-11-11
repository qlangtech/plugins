package com.qlangtech.tis.plugin.ontology.workshop.widget.component;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.ibatis.RowMap;
import com.qlangtech.tis.plugin.ontology.AggregationFunc;
import com.qlangtech.tis.plugin.ontology.OntologyProperty;
import com.qlangtech.tis.plugin.ontology.OntologyType;
import com.qlangtech.tis.plugin.ontology.aggregation.AggregationCriteria;
import com.qlangtech.tis.plugin.ontology.aggregation.AggregationResult;
import com.qlangtech.tis.plugin.ontology.aggregation.GroupKey;
import com.qlangtech.tis.plugin.ontology.impl.objtype.FilterOperator;
import com.qlangtech.tis.plugin.ontology.impl.objtype.ObjectTypeBinding;
import com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopVariable;
import com.qlangtech.tis.plugin.ontology.workshop.model.variable.ObjectSetVariable;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.FilterListWidget;
import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * 过滤列表里 histogram 控件的取数器 —— 把「某个数值属性怎么分布」变成一根根柱子。
 *
 * <h3>为什么是两趟聚合</h3>
 * 分桶需要先知道<b>桶宽</b>，而桶宽取决于数据的 {@code MIN / MAX}；不先取一次极值，就只能
 * 在浏览器里先拉全部明细再切桶 —— 那在大表上会被 {@code maxGroups} 截断成一份残缺分布，
 * 而且残缺得很安静（图表看起来完全正常）。
 *
 * <ol>
 *   <li><b>第一趟</b>：不带分组键的标量聚合 {@code select MIN(x), MAX(x) from t where …}
 *       —— 恒一行，行里只有 {@code m0} / {@code m1}；</li>
 *   <li><b>第二趟</b>：{@code GroupKey.numeric(x, width)} + {@code COUNT(*)}，下推成
 *       {@code select FLOOR(x / width) as c0, COUNT(*) as m0 … group by 1}。</li>
 * </ol>
 * 两趟都走 {@link ObjectSetVariable#executeAggregation}，因此对象集自带的<b>静态过滤条件</b>
 * 自动施加，不需要本类重复。
 *
 * <h3>为什么桶从 0 对齐而不是从 min 对齐</h3>
 * SQL 侧只渲染 {@code FLOOR(col / width)}，没有偏移量；要让桶从 min 起算就得再给
 * {@link GroupKey} 加一个 offset 分量，并把「数据集自己的偏移」塞进 SQL 表达式。代价是每改一次
 * 静态过滤条件，桶边界就整体平移一次 —— 用户会看到「同样的柱子换了个位置」。
 * 从 0 对齐是稳定的：取值范围变了只影响柱子根数，不影响既有边界。
 *
 * <h3>柱数与「桶数」不是同一个数</h3>
 * {@link HistogramComponent#bucketCount} 决定的是<b>分桶精度</b>（桶宽 = 值域 / 柱数），
 * 实际渲染的柱子<b>恰好等于</b>柱数：首桶到末桶之间的越界索引会被折进末桶（见
 * {@link #foldCounts}），因此最后一根柱子在语义上是左闭右闭 {@code [lower, +∞)}。
 * 前端对最后一根必须用 {@code <=} 而不是 {@code <}，否则恰好落在最大值上的那些行会被漏掉。
 *
 * <h3>响应契约</h3>
 * <pre>
 * {
 *   "property": "amount",
 *   "count": 1234,          // 参与统计的行数（该属性非空的行）
 *   "min": 0.0, "max": 1000.0, "bucketWidth": 100.0,
 *   "buckets": [ {"lower": 0.0, "upper": 100.0, "count": 12}, … ]   // 恒为 bucketCount 根，空桶也在
 * }
 * </pre>
 * 空桶<b>不能省略</b>：直方图的形状就是由「这里没有数据」表达出来的，把空桶丢掉会得到一张
 * 看起来连续但实际上断断续续的图。
 *
 * <h3>为什么不自己去找「是哪一行」</h3>
 * 调用方 {@link HistogramComponent#createPayloadProps} 从祖先链里已经拿到了行
 * （{@code FilterItemConfig}）与宿主（{@code FilterListWidget}），本类只吃纯输入。
 * 早先的 {@code process(widget, property)} 要回头遍历 {@code widget.filters} 反查行，
 * 那是「端点靠 query 参数指路」时代的产物。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/11/4
 * @see HistogramComponent
 */
public final class HistogramDataProcessor {

    /**
     * 能做数值分桶的本体类型。
     *
     * <p>刻意<b>不含</b> DATE / TIMESTAMP：时间的「区间」有确定语义（天 / 小时），已经由
     * {@code date-picker} + 图表刷选承担；把一个时间戳按「每 86400 秒一根柱子」切开，用户
     * 读到的刻度是「第 20000 天」这种谁都不想看的数字。
     */
    private static final Set<OntologyType> BUCKETABLE_TYPES = Set.of(
            OntologyType.INTEGER, OntologyType.SHORT, OntologyType.BYTE,
            OntologyType.LONG, OntologyType.FLOAT, OntologyType.DOUBLE, OntologyType.DECIMAL);

    /** 桶边界保留的最小小数位：整数列算出来的桶宽常是 3.3333…，无限精度既没必要也没法展示 */
    private static final int BOUND_SCALE = 6;

    private HistogramDataProcessor() {
    }

    /**
     * 取某个过滤项的数值分布。
     *
     * @param widget      宿主 widget，本类从它取 {@code objectSetVar}
     * @param property    要画分布的本体属性名（= 过滤项行的 {@code property}）
     * @param bucketCount 柱数（调用方已经用 {@link HistogramComponent#resolveBucketCount()} 归一过）
     * @return 见类注释的响应契约
     * @throws IllegalStateException 属性不在本体里、属性不是数值型、或变量不是对象集变量
     */
    public static JSONObject process(FilterListWidget widget, String property, int bucketCount) {
        Objects.requireNonNull(widget, "widget can not be null");
        if (StringUtils.isBlank(property)) {
            throw new IllegalArgumentException("histogram property can not be blank");
        }
        ObjectSetVariable objSetVar = resolveObjectSetVariable(widget);

        OntologyType propType = requireBucketableProperty(objSetVar.getColsMeta(), property);
        if (!BUCKETABLE_TYPES.contains(propType)) {
            throw new IllegalStateException("property:'" + property + "' type:" + propType
                    + " can not be rendered as a histogram, only numeric properties support it");
        }

        return build(objSetVar, property, Math.max(bucketCount, HistogramComponent.MIN_BUCKET_COUNT));
    }

    private static JSONObject build(ObjectSetVariable objSetVar, String property, int bucketCount) {
        JSONObject result = new JSONObject();
        result.put("property", property);
        result.put("bucketCount", bucketCount);

        // ---- 第一趟：标量聚合取极值 ----
        AggregationCriteria rangeCriteria = new AggregationCriteria();
        rangeCriteria.addTargetCol(property);
        rangeCriteria.addMetric(AggregationFunc.MIN, property);
        rangeCriteria.addMetric(AggregationFunc.MAX, property);
        AggregationResult range = objSetVar.executeAggregation(rangeCriteria);
        List<RowMap> rangeRows = range.rows();
        if (rangeRows.isEmpty()) {
            // 标量聚合恒返回一行；一行都没有说明驱动/方言层出了偏差，别把它伪装成「没有数据」
            throw new IllegalStateException("scalar aggregation for property:'" + property
                    + "' returned no row, expected exactly one");
        }
        RowMap rangeRow = rangeRows.get(0);
        Object rawMin = rangeRow.get(AggregationCriteria.metricAlias(0));
        Object rawMax = rangeRow.get(AggregationCriteria.metricAlias(1));
        if (!(rawMin instanceof Number minValue) || !(rawMax instanceof Number maxValue)) {
            // 全表该属性都是 NULL（或表为空）：合法但无内容，返回全空桶让前端显示空态
            result.put("min", null);
            result.put("max", null);
            result.put("bucketWidth", null);
            result.put("count", 0L);
            result.put("buckets", new JSONArray());
            return result;
        }

        final double min = minValue.doubleValue();
        final double max = maxValue.doubleValue();
        final double width = (max - min) / bucketCount;
        result.put("min", min);
        result.put("max", max);

        if (!(width > 0d) || Double.isInfinite(width)) {
            // 全部取值相同（min == max）：值域为 0，分桶没有意义。退化成一个覆盖全体的单桶，
            // 而不是「桶宽 0 → 除零」或「空图」—— 用户看到的是「全都是这个值」，这是事实。
            long total = countAll(objSetVar, property, min);
            result.put("bucketWidth", 0d);
            result.put("count", total);
            JSONArray single = new JSONArray();
            single.add(bucket(min, min, total));
            result.put("buckets", single);
            return result;
        }
        result.put("bucketWidth", width);

        // ---- 第二趟：按 FLOOR(x / width) 分组计数 ----
        AggregationCriteria countCriteria = new AggregationCriteria();
        countCriteria.addTargetCol(property);
        countCriteria.addGroupKey(GroupKey.numeric(property, width, 1));
        countCriteria.addMetric(AggregationFunc.COUNT, null);
        // floor(max/width) - floor(min/width) 恒等于 bucketCount，故最多 bucketCount+1 组；
        // 多取一组只是为了「真出现第 +2 组时不要被 LIMIT 悄悄抹掉」
        countCriteria.setMaxGroups(bucketCount + 2);
        AggregationResult counted = objSetVar.executeAggregation(countCriteria);

        final long firstIndex = (long) Math.floor(min / width);
        long[] counts = new long[bucketCount];
        long total = 0L;
        for (RowMap row : counted.rows()) {
            Object rawIndex = row.get(AggregationCriteria.groupAlias(0));
            Object rawCount = row.get(AggregationCriteria.metricAlias(0));
            if (!(rawIndex instanceof Number index) || !(rawCount instanceof Number count)) {
                continue;
            }
            long slot = index.longValue() - firstIndex;
            counts[foldCounts(slot, bucketCount)] += count.longValue();
            total += count.longValue();
        }
        result.put("count", total);

        BigDecimal widthDecimal = BigDecimal.valueOf(width);
        BigDecimal origin = widthDecimal.multiply(BigDecimal.valueOf(firstIndex));
        JSONArray buckets = new JSONArray(bucketCount);
        for (int i = 0; i < bucketCount; i++) {
            BigDecimal lower = origin.add(widthDecimal.multiply(BigDecimal.valueOf(i)));
            BigDecimal upper = lower.add(widthDecimal);
            buckets.add(bucket(lower.setScale(BOUND_SCALE, RoundingMode.HALF_UP).stripTrailingZeros().doubleValue(),
                    upper.setScale(BOUND_SCALE, RoundingMode.HALF_UP).stripTrailingZeros().doubleValue(),
                    counts[i]));
        }
        result.put("buckets", buckets);
        return result;
    }

    /**
     * 把分组索引折进 {@code [0, bucketCount-1]}。
     *
     * <p>只有末桶会真的发生折叠：{@code floor(max/width) - floor(min/width) == bucketCount}，
     * 也就是最顶端那一组永远比柱子多一格。折进末桶而不是「多渲染一根」是有意的 ——
     * 柱数是用户在配置里填的那个数，多出来一根既对不上配置，也让点击目标多一个。
     *
     * <p>包级可见<b>为了可测</b>：这是「请求几根柱子就得到几根」的唯一保证，单测直接打表。
     */
    static int foldCounts(long slot, int bucketCount) {
        if (slot < 0) {
            return 0;
        }
        return (int) Math.min(slot, bucketCount - 1L);
    }

    /**
     * 值域退化成单桶时的行数。
     *
     * <p>必须加 {@code property >= min} 而不是裸 {@code COUNT(*)}：第二趟分组的第一个分组键自带
     * {@code IS NOT NULL}，NULL 行不计入柱子；这里若把 NULL 也算上，单桶图的总数就会比正常情形
     * 更多，两幅图的 y 轴口径不一致。{@code >=} 同时完成「排 NULL」与「取值相同」两件事。
     */
    private static long countAll(ObjectSetVariable objSetVar, String property, double min) {
        AggregationCriteria criteria = new AggregationCriteria();
        criteria.addTargetCol(property);
        criteria.addFilter(property, FilterOperator.greaterThanOrEqual, min);
        criteria.addMetric(AggregationFunc.COUNT, null);
        AggregationResult result = objSetVar.executeAggregation(criteria);
        if (result.rows().isEmpty()) {
            return 0L;
        }
        Object raw = result.rows().get(0).get(AggregationCriteria.metricAlias(0));
        return raw instanceof Number number ? number.longValue() : 0L;
    }

    private static JSONObject bucket(double lower, double upper, long count) {
        JSONObject bucket = new JSONObject();
        bucket.put("lower", lower);
        bucket.put("upper", upper);
        bucket.put("count", count);
        return bucket;
    }

    private static ObjectSetVariable resolveObjectSetVariable(FilterListWidget widget) {
        if (StringUtils.isBlank(widget.objectSetVar)) {
            throw new IllegalStateException("filter list widget objectSetVar can not be blank");
        }
        WorkshopVariable variable = WorkshopVariable.loadDetail(widget.objectSetVar);
        if (!(variable instanceof ObjectSetVariable objSetVar)) {
            throw new IllegalStateException("filter list widget objectSetVar:'" + widget.objectSetVar
                    + "' is not an object set variable");
        }
        return objSetVar;
    }

    /**
     * 属性必须在本体里存在 —— 与 {@code AggregationPushdownPlanner#requireCol} 同一条边界：
     * 请求里的属性名不能直接进 SQL，白名单只能是对象类型自己声明的属性。
     */
    private static OntologyType requireBucketableProperty(List<OntologyProperty> colsMeta, String property) {
        for (OntologyProperty prop : colsMeta) {
            if (property.equals(prop.getName())) {
                return prop.parseOntologyType();
            }
        }
        throw new IllegalStateException("property:'" + property + "' does not exist in object set properties:"
                + colsMeta.stream().map(OntologyProperty::getName).toList());
    }
}
