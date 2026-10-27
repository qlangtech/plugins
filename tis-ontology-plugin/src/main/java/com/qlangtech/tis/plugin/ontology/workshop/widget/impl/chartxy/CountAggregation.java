package com.qlangtech.tis.plugin.ontology.workshop.widget.impl.chartxy;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;

/**
 * 计数聚合（默认）。
 *
 * <p>计数不依赖度量属性，故 {@code CountAggregation.json} 把继承来的
 * {@code property} 标为 {@code disable}（从表单移除）。注意这只是<b>界面</b>约束：
 * {@code AbstractSeriesAggregation.property} 仍存在，落盘时值为 null。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/24
 */
public class CountAggregation extends AbstractSeriesAggregation {

    private static final long serialVersionUID = 1L;

    @TISExtension
    public static class DefaultDescriptor extends BasicDescriptor implements DescriptorUseableShortComment {
        @Override
        public String getDisplayName() {
            return "Count";
        }

        @Override
        public String shortComment() {
            return "计数";
        }
    }
}
