package com.qlangtech.tis.plugin.ontology.workshop.widget.impl.chartxy;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;

/**
 * 去重计数聚合。
 *
 * <p>聚合已下推到数据库执行，语义随方言而异：
 * <ul>
 *   <li>Doris / StarRocks：{@code APPROX_COUNT_DISTINCT}，结果是<b>统计估计</b>，
 *       同一个数两次查询可能有细微差异；</li>
 *   <li>MySQL / MariaDB / TiDB / OceanBase：没有近似函数，回落精确的
 *       {@code COUNT(DISTINCT)}。</li>
 * </ul>
 * 这是「大表上能用」的必要取舍，两类结果不应被期望逐值一致。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/24
 */
public class ApproxUniqueCountAggregation extends AbstractSeriesAggregation {

    private static final long serialVersionUID = 1L;

    @TISExtension
    public static class DefaultDescriptor extends BasicDescriptor implements DescriptorUseableShortComment {
        @Override
        public String getDisplayName() {
            return "Approx Unique Count";
        }

        @Override
        public String shortComment() {
            return "近似去重计数";
        }
    }
}
