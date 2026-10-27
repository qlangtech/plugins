package com.qlangtech.tis.plugin.ontology.workshop.widget.impl.chartxy;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;

/**
 * 平均值聚合。分母是<b>有效值</b>个数：null 与无法解析的值既不进分子也不进分母。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/24
 */
public class AverageAggregation extends AbstractSeriesAggregation {

    private static final long serialVersionUID = 1L;

    @TISExtension
    public static class DefaultDescriptor extends BasicDescriptor implements DescriptorUseableShortComment {
        @Override
        public String getDisplayName() {
            return "Average";
        }

        @Override
        public String shortComment() {
            return "平均值";
        }
    }
}
