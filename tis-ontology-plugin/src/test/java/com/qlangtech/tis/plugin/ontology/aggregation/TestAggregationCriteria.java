package com.qlangtech.tis.plugin.ontology.aggregation;

import com.qlangtech.tis.plugin.ontology.AggregationFunc;
import org.junit.Assert;
import org.junit.Test;

/**
 * 核心 API 的结构化约束：别名契约、白名单边界、参数边界。
 *
 * <p>这些约束是 SQL 注入防线的第一层 —— 拼 SQL 之前就不再接受任何「裸字符串」，
 * 只接受经过正则与 targetCols 双重校验的标识符。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/24
 */
public class TestAggregationCriteria {

    @Test
    public void aliasesArePositional() {
        Assert.assertEquals("c0", AggregationCriteria.GROUP_ALIAS);
        Assert.assertEquals("c1", AggregationCriteria.SEGMENT_ALIAS);
        Assert.assertEquals("m0", AggregationCriteria.metricAlias(0));
        Assert.assertEquals("m7", AggregationCriteria.metricAlias(7));
    }

    @Test(expected = IllegalArgumentException.class)
    public void groupAliasRejectsThirdKey() {
        AggregationCriteria.groupAlias(2);
    }

    @Test
    public void groupKeyRequiresTargetCol() {
        AggregationCriteria criteria = new AggregationCriteria();
        try {
            criteria.addGroupKey(GroupKey.rawColumn("status"));
            Assert.fail("未声明为 targetCol 的列不能进分组键");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("targetCols"));
        }
        criteria.addTargetCol("status");
        criteria.addGroupKey(GroupKey.rawColumn("status"));
        Assert.assertEquals(1, criteria.getGroupKeys().size());
    }

    @Test
    public void metricRequiresTargetCol() {
        AggregationCriteria criteria = new AggregationCriteria();
        try {
            criteria.addMetric(AggregationFunc.SUM, "amount");
            Assert.fail("未声明为 targetCol 的列不能进指标");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("targetCols"));
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void illegalColumnNameIsRejectedByRegex() {
        // 转义只是加引号，挡不住自带引号的列名 —— 必须在构造期用正则拒绝
        GroupKey.rawColumn("status`; drop table t; --");
    }

    @Test(expected = IllegalArgumentException.class)
    public void countRejectsMetricProperty() {
        new AggregationCriteria.Metric(AggregationFunc.COUNT, "status");
    }

    @Test
    public void metricsAreDeduped() {
        AggregationCriteria criteria = new AggregationCriteria();
        criteria.addTargetCol("amount");
        criteria.addMetric(AggregationFunc.SUM, "amount");
        criteria.addMetric(AggregationFunc.SUM, "amount");
        Assert.assertEquals(1, criteria.getMetrics().size());
        Assert.assertEquals(0, criteria.metricIndex(AggregationFunc.SUM, "amount"));
    }

    @Test
    public void maxGroupsBounds() {
        AggregationCriteria criteria = new AggregationCriteria();
        Assert.assertEquals(AggregationCriteria.DEFAULT_MAX_GROUPS, criteria.getMaxGroups());
        criteria.setMaxGroups(AggregationCriteria.MAX_MAX_GROUPS);
        try {
            criteria.setMaxGroups(0);
            Assert.fail("maxGroups 不能为 0");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("maxGroups"));
        }
        try {
            criteria.setMaxGroups(AggregationCriteria.MAX_MAX_GROUPS + 1);
            Assert.fail("maxGroups 不能超过上限");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("maxGroups"));
        }
    }

    @Test
    public void atMostTwoGroupKeys() {
        AggregationCriteria criteria = new AggregationCriteria();
        criteria.addTargetCol("a");
        criteria.addTargetCol("b");
        criteria.addTargetCol("c");
        criteria.addGroupKey(GroupKey.rawColumn("a"));
        criteria.addGroupKey(GroupKey.rawColumn("b"));
        try {
            criteria.addGroupKey(GroupKey.rawColumn("c"));
            Assert.fail("最多 x + segment 两个分组键");
        } catch (IllegalStateException e) {
            Assert.assertTrue(e.getMessage().contains("at most"));
        }
    }
}
