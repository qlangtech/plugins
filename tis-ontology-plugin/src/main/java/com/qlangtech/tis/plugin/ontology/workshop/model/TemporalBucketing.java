package com.qlangtech.tis.plugin.ontology.workshop.model;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.aggregation.TimeGranularity;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * 按时间粒度分桶（Temporal Bucketing）
 *
 * <p>适用于 X 轴属性为<b>日期或时间戳</b>的图层。两个字段合起来表达一个时间刻度：
 * <b>粒度</b>（切到哪一级）与<b>粒度单位数</b>（一个刻度跨几级）。
 *
 * <pre>
 *   granularity=DAY,   unitValue=1  →  每天一个刻度        （byDays）
 *   granularity=DAY,   unitValue=7  →  每 7 天一个刻度     （byDays(7)）
 *   granularity=HOUR,  unitValue=6  →  每 6 小时一个刻度   （byHours(6)）
 * </pre>
 *
 * <p>两个字段必须成对存在：只有粒度没有单位数，表达不了「双周」「季度汇总」这类最常见的
 * 业务刻度；只有单位数没有粒度，连单位是什么都不知道。这正是它们同属一个子类、
 * 且不该被压成「一个粒度枚举塞满 周/双周/月/季度/年」的原因——后者会让枚举值随组合爆炸。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/15
 * @see XAxisBucketing
 */
public class TemporalBucketing extends XAxisBucketing {

    private static final long serialVersionUID = 1L;

    /**
     * 时间粒度 —— 刻度切到哪一级；默认「天」。
     */
    @FormField(ordinal = 10, type = FormFieldType.ENUM, validate = {Validator.require})
    public TimeGranularity granularity = TimeGranularity.DAY;

    /**
     * 粒度单位数 —— 一个刻度包含几个粒度单位；默认 1（即每 1 个粒度一个刻度）。
     */
    @FormField(ordinal = 11, type = FormFieldType.INT_NUMBER, validate = {Validator.require, Validator.integer})
    public Integer unitValue = 1;

    // ------------------------------------------------------------------
    // 分组值 → 分桶键 —— 序号还原与标签格式化都在本类闭环
    // ------------------------------------------------------------------

    /** 按天分桶的对齐基准（只用于还原桶起点）。 */
    private static final LocalDateTime DAY_EPOCH = LocalDate.of(1970, 1, 1).atStartOfDay();

    /** 按周分桶的对齐基准，取 1970-01-05（周一），与 SQL 侧的 {@code DATEDIFF(c, '1970-01-05')} 一致。 */
    private static final LocalDateTime WEEK_EPOCH = LocalDate.of(1970, 1, 5).atStartOfDay();

    /**
     * 各粒度的标签格式。{@link DateTimeFormatter} 不可变且线程安全，声明为常量复用；
     * 周与天共用日期格式（周桶起点固定为周一），时与分共用 {@code yyyy-MM-dd'T'HH:mm}
     * （时桶的分钟恒为 0，尾部 {@code :00} 保留是为了与秒级格式保持同一种可读形态）。
     */
    private static final DateTimeFormatter YEAR_LABEL = DateTimeFormatter.ofPattern("yyyy");
    private static final DateTimeFormatter QUARTER_LABEL = DateTimeFormatter.ofPattern("yyyy-'Q'Q");
    private static final DateTimeFormatter MONTH_LABEL = DateTimeFormatter.ofPattern("yyyy-MM");
    private static final DateTimeFormatter DATE_LABEL = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter HOUR_LABEL = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm");
    private static final DateTimeFormatter SECOND_LABEL = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");

    /**
     * 数据库返回的对齐序号 → 分桶键。
     *
     * <p>SQL 侧（{@code MySqlAggregationSqlBuilder#renderGroupKey}）返回的是<b>对齐后的
     * 整数序号</b>而不是时间戳，本方法负责把它翻译回日期再格式化成标签。序号公式与
     * {@link #bucketStart} 必须成对修改。绝不能把序号交给「按 epoch 毫秒解析」的逻辑，
     * 否则 20000 天会被解释成 1970-01-01 00:00:20。
     *
     * <p>标签精度跟随 {@link #granularity}：天粒度只输出日期（{@code 2026-10-01}），
     * 月粒度到年月（{@code 2026-10}），季度带季次（{@code 2026-Q4}）。
     * 前端把它当不透明类目字符串直显，样式即契约。
     */
    @Override
    public BucketKey format(Object rawGroup) {
        // 粒度/单位数为空只可能来自历史落盘数据（表单侧已 require 校验），兜底与
        // AggregationPushdownPlanner 对空值的既有约定一致
        TimeGranularity effective = granularity == null ? TimeGranularity.DAY : granularity;
        int unit = unitValue == null || unitValue < 1 ? 1 : unitValue;
        long alignedIndex = Math.floorDiv(((Number) rawGroup).longValue(), unit) * unit;
        LocalDateTime bucket = bucketStart(effective, alignedIndex);
        return new BucketKey(bucket.format(bucketFormatter(effective)), bucket);
    }

    /**
     * 由 SQL 返回的桶序号还原桶起点。
     *
     * <p>序号公式见 {@code MySqlAggregationSqlBuilder#renderGroupKey}：SQL 只返回
     * {@code FLOOR(index/unit)*unit}，本方法负责把它翻译回日期。两边必须成对修改。
     *
     * <p>公开静态<b>为了可测</b>：这是 SQL 与 Java 之间唯一的语义接缝，
     * 单测用黄金日期断言，不依赖旧的截断算法。
     */
    public static LocalDateTime bucketStart(TimeGranularity granularity, long index) {
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

    private static DateTimeFormatter bucketFormatter(TimeGranularity granularity) {
        return switch (granularity) {
            case YEAR -> YEAR_LABEL;
            case QUARTER -> QUARTER_LABEL;
            case MONTH -> MONTH_LABEL;
            case WEEK, DAY -> DATE_LABEL;
            case HOUR, MINUTE -> HOUR_LABEL;
            case SECOND -> SECOND_LABEL;
        };
    }

    // TimeGranularity 已提升到
    // com.qlangtech.tis.plugin.ontology.aggregation.TimeGranularity：
    // 核心层的聚合 API（GroupKey）也要表达「按哪一级粒度切桶」，而核心层不该依赖本插件
    // 工程的表单模型。常量名与 shortComment 原样保留，XStream / 表单选项按常量名持久化，
    // 存量配置不受影响。时 / 分 / 秒仅时间戳属性的约束由使用侧（聚合下推 planner）校验。

    @TISExtension
    public static class DefaultDescriptor extends BasicDescriptor implements DescriptorUseableShortComment {

        @Override
        public String getDisplayName() {
            return "Temporal Bucketing";
        }

        @Override
        public String shortComment() {
            return "按时间粒度分桶";
        }

        // validateLimit（桶数上限）由 BasicDescriptor 提供，此处无需重声明 —— 见那里的说明。

        /**
         * 粒度单位数必须为正整数：0 会让每个刻度宽度为零，负值无意义。
         */
        public boolean validateUnitValue(IFieldErrorHandler msgHandler, Context context,
                                         String fieldName, String value) {
            int val = Integer.parseInt(value);
            if (val < 1) {
                msgHandler.addFieldError(context, fieldName, "必须大于 0");
                return false;
            }
            return true;
        }
    }
}
