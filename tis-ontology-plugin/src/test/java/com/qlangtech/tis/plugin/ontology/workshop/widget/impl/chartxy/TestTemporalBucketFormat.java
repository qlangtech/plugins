package com.qlangtech.tis.plugin.ontology.workshop.widget.impl.chartxy;

import com.qlangtech.tis.plugin.ontology.aggregation.TimeGranularity;
import com.qlangtech.tis.plugin.ontology.workshop.model.TemporalBucketing;
import org.junit.Assert;
import org.junit.Test;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;

/**
 * {@link TemporalBucketing#format} 的黄金标签断言 —— SQL 对齐序号 → 标签，
 * 精度跟随分桶粒度，天粒度的桶不应拖着一串 {@code T00:00:00} 出现在坐标轴上。
 *
 * <p>输入是 SQL 返回的对齐序号（序号公式与 {@code MySqlAggregationSqlBuilder#renderGroupKey}
 * 成对，序号→桶起点的黄金日期见 {@code TestTemporalBucketStart}），这里验证
 * 「序号 → 标签」全链路。前端把标签当不透明类目字符串直显，样式即契约 ——
 * 改这里的期望值等于改用户看到的轴标签。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/11/14
 */
public class TestTemporalBucketFormat {

    @Test
    public void yearLabelIsBareYear() {
        assertLabel(TimeGranularity.YEAR, 2026, "2026");
    }

    @Test
    public void quarterLabelCarriesQuarterNumber() {
        // 2026 年 Q4 的序号 = 2026 * 4 + 3
        assertLabel(TimeGranularity.QUARTER, 2026L * 4 + 3, "2026-Q4");
    }

    @Test
    public void monthLabelIsYearMonth() {
        assertLabel(TimeGranularity.MONTH, 2026L * 12 + 9, "2026-10");
    }

    @Test
    public void weekLabelIsBucketMonday() {
        // 周桶起点固定为周一（对齐基准 1970-01-05），标签就是那个周一
        Assert.assertTrue("前提：2026-10-05 是周一",
                LocalDate.of(2026, 10, 5).getDayOfWeek() == DayOfWeek.MONDAY);
        long index = ChronoUnit.WEEKS.between(
                LocalDate.of(1970, 1, 5), LocalDate.of(2026, 10, 5));
        assertLabel(TimeGranularity.WEEK, index, "2026-10-05");
    }

    @Test
    public void dayLabelHasNoTimeSuffix() {
        assertLabel(TimeGranularity.DAY, dayIndex(2026, 10, 1), "2026-10-01");
    }

    @Test
    public void subDayLabelsKeepIsoLocalDateTimeShape() {
        long hourIndex = dayIndex(2026, 10, 1) * 24 + 14;
        assertLabel(TimeGranularity.HOUR, hourIndex, "2026-10-01T14:00");
        assertLabel(TimeGranularity.MINUTE, hourIndex * 60 + 30, "2026-10-01T14:30");
        assertLabel(TimeGranularity.SECOND, hourIndex * 3600 + 30 * 60 + 59, "2026-10-01T14:30:59");
    }

    @Test
    public void nullGranularityFallsBackToDay() {
        // 粒度为空只可能来自历史落盘数据（表单侧已 require 校验），兜底为「天」
        TemporalBucketing bucketing = new TemporalBucketing();
        bucketing.granularity = null;
        Assert.assertEquals("2026-10-01", bucketing.format(dayIndex(2026, 10, 1)).label());
    }

    @Test
    public void unitValueBelowOneIsNormalizedToOne() {
        // 单位数为空 / <1 只可能来自历史落盘数据，兜底为 1 —— 每个序号自成一刻度
        TemporalBucketing bucketing = new TemporalBucketing();
        bucketing.unitValue = null;
        Assert.assertEquals("2026-10-01", bucketing.format(dayIndex(2026, 10, 1)).label());
        bucketing.unitValue = 0;
        Assert.assertEquals("2026-10-02", bucketing.format(dayIndex(2026, 10, 2)).label());
    }

    private static void assertLabel(TimeGranularity granularity, long index, String expected) {
        TemporalBucketing bucketing = new TemporalBucketing();
        bucketing.granularity = granularity;
        var key = bucketing.format(index);
        Assert.assertEquals("granularity=" + granularity, expected, key.label());
        Assert.assertNotNull("时间桶必须带桶起点（排序依据）", key.time());
    }

    private static long dayIndex(int year, int month, int day) {
        return ChronoUnit.DAYS.between(LocalDate.of(1970, 1, 1), LocalDate.of(year, month, day));
    }
}
