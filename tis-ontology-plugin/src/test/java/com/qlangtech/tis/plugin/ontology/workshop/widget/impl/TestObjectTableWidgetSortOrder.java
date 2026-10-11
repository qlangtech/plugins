package com.qlangtech.tis.plugin.ontology.workshop.widget.impl;

import org.junit.Assert;
import org.junit.Test;

/**
 * 列排序请求参数的方向解析契约（{@link ObjectTableWidget.DescriptorImpl#parseSortOrderAsc(String)}）。
 *
 * <p>这是「列排序」这条协议里唯一**拼错了也照样出数据**的地方 —— 把 `descend` 静默当成升序，
 * 结果是数据出来了、排序箭头也亮着，只是方向反了，从界面上看不出来。故单独钉住。
 *
 * <p>纯逻辑断言，不触达 TIS 插件扫描，故无环境前提。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/9
 */
public class TestObjectTableWidgetSortOrder {

    @Test
    public void testAsc() {
        Assert.assertTrue(ObjectTableWidget.DescriptorImpl.parseSortOrderAsc("asc"));
        Assert.assertTrue("大小写不敏感", ObjectTableWidget.DescriptorImpl.parseSortOrderAsc("ASC"));
    }

    @Test
    public void testDesc() {
        Assert.assertFalse(ObjectTableWidget.DescriptorImpl.parseSortOrderAsc("desc"));
        Assert.assertFalse("大小写不敏感", ObjectTableWidget.DescriptorImpl.parseSortOrderAsc("Desc"));
    }

    /** 只给了排序列没给方向：按升序（前端两者总是一起发，这是宽容分支） */
    @Test
    public void testBlankDirectionFallsBackToAsc() {
        Assert.assertTrue(ObjectTableWidget.DescriptorImpl.parseSortOrderAsc(null));
        Assert.assertTrue(ObjectTableWidget.DescriptorImpl.parseSortOrderAsc(""));
        Assert.assertTrue(ObjectTableWidget.DescriptorImpl.parseSortOrderAsc("   "));
    }

    /**
     * 非法方向必须抛，不能静默当升序：`descend`（ng-zorro 的词汇）是最可能被写错的一个 ——
     * 前端接口用的是 asc/desc，若有人按 ng-zorro 的习惯拼参数，这里要立刻响。
     */
    @Test
    public void testIllegalDirectionThrows() {
        for (String illegal : new String[]{"descend", "ascend", "desc asc", "up", "1"}) {
            try {
                ObjectTableWidget.DescriptorImpl.parseSortOrderAsc(illegal);
                Assert.fail("illegal sortOrder should be rejected:" + illegal);
            } catch (IllegalArgumentException e) {
                Assert.assertTrue(e.getMessage().contains(illegal));
            }
        }
    }
}
