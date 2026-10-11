package com.qlangtech.tis.plugin.ontology.workshop.model.section;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;

/**
 * 行布局：子分区上下堆叠，每行一个子分区。
 *
 * <p>本布局没有任何可配参数，类体为空。之所以仍然建一个子类而不是省掉它：
 * 布局身份由 Java 类型承担，六个布局必须一一对应六个子类，
 * 否则编译器就无从区分「行布局」与「工具栏布局」——那正是本次重构要消除的、
 * 旁挂枚举所带来的重复分类体系。
 */
public class RowsLayout extends SectionLayout {

    @TISExtension
    public static class DefaultDescriptor extends BasicDescriptor
            implements DescriptorUseableShortComment {
        @Override
        public String shortComment() {
            return "行布局";
        }

        @Override
        public String getDisplayName() {
            return "Rows Layout";
        }
    }
}
