package com.qlangtech.tis.plugin.ontology.workshop.widget.impl.chartxy;

import com.qlangtech.tis.plugin.ontology.aggregation.TimeGranularity;
import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDateTime;

/**
 * SQL 桶序号 → 桶起点。
 *
 * <p>这是 SQL 与 Java 之间唯一的语义接缝：SQL 只返回整数序号（{@code FLOOR(index/unit)*unit}），
 * 由 {@link ChartXYDataProcessor#bucketStart} 还原成日期。两边公式必须成对修改，
 * 因此这里用<b>黄金日期</b>逐粒度断言，而不是拿旧的内存截断算法做对照。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/24
 */
public class TestTemporalBucketStart {

    @Test
    public void year() {
        Assert.assertEquals(LocalDateTime.of(2026, 1, 1, 0, 0),
                ChartXYDataProcessor.bucketStart(TimeGranularity.YEAR, 2026));
    }

    @Test
    public void quarter() {
        // 序号 = year*4 + quarterIndex（0 起）
        Assert.assertEquals(LocalDateTime.of(2026, 1, 1, 0, 0),
                ChartXYDataProcessor.bucketStart(TimeGranularity.QUARTER, 2026L * 4));
        Assert.assertEquals(LocalDateTime.of(2026, 4, 1, 0, 0),
                ChartXYDataProcessor.bucketStart(TimeGranularity.QUARTER, 2026L * 4 + 1));
        Assert.assertEquals(LocalDateTime.of(2026, 10, 1, 0, 0),
                ChartXYDataProcessor.bucketStart(TimeGranularity.QUARTER, 2026L * 4 + 3));
    }

    @Test
    public void month() {
        // 序号 = year*12 + (month-1)
        Assert.assertEquals(LocalDateTime.of(2026, 1, 1, 0, 0),
                ChartXYDataProcessor.bucketStart(TimeGranularity.MONTH, 2026L * 12));
        Assert.assertEquals(LocalDateTime.of(2026, 12, 1, 0, 0),
                ChartXYDataProcessor.bucketStart(TimeGranularity.MONTH, 2026L * 12 + 11));
    }

    @Test
    public void weekIsAnchoredAtMonday19700105() {
        Assert.assertEquals(LocalDateTime.of(1970, 1, 5, 0, 0),
                ChartXYDataProcessor.bucketStart(TimeGranularity.WEEK, 0));
        Assert.assertEquals(LocalDateTime.of(1970, 1, 12, 0, 0),
                ChartXYDataProcessor.bucketStart(TimeGranularity.WEEK, 1));
    }

    @Test
    public void dayIsAnchoredAt19700101() {
        Assert.assertEquals(LocalDateTime.of(1970, 1, 1, 0, 0),
                ChartXYDataProcessor.bucketStart(TimeGranularity.DAY, 0));
        Assert.assertEquals(LocalDateTime.of(2026, 1, 1, 0, 0),
                ChartXYDataProcessor.bucketStart(TimeGranularity.DAY, 20454));
    }

    @Test
    public void negativeIndexFloorsTowardsPast() {
        // FLOOR 语义：1970 之前也要落在正确的桶边界，不能向 0 漂移
        Assert.assertEquals(LocalDateTime.of(1969, 12, 31, 0, 0),
                ChartXYDataProcessor.bucketStart(TimeGranularity.DAY, -1));
        Assert.assertEquals(LocalDateTime.of(1969, 12, 29, 0, 0),
                ChartXYDataProcessor.bucketStart(TimeGranularity.WEEK, -1));
    }

    @Test
    public void hourMinuteSecondShareDayZero() {
        Assert.assertEquals(LocalDateTime.of(1970, 1, 1, 0, 0),
                ChartXYDataProcessor.bucketStart(TimeGranularity.HOUR, 0));
        Assert.assertEquals(LocalDateTime.of(1970, 1, 2, 0, 0),
                ChartXYDataProcessor.bucketStart(TimeGranularity.HOUR, 24));
        Assert.assertEquals(LocalDateTime.of(1970, 1, 1, 1, 30),
                ChartXYDataProcessor.bucketStart(TimeGranularity.MINUTE, 90));
        Assert.assertEquals(LocalDateTime.of(1970, 1, 1, 0, 1, 30),
                ChartXYDataProcessor.bucketStart(TimeGranularity.SECOND, 90));
    }
}
