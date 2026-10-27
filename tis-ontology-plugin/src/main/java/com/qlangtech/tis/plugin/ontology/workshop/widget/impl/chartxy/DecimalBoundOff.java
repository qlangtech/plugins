package com.qlangtech.tis.plugin.ontology.workshop.widget.impl.chartxy;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;

/**
 * 数值轴边界的「自动计算」实现：零字段，边界由图表实际数据决定。
 *
 * <p>沿用改造前的默认（{@code autoMinBound = true} / {@code autoMaxBound = true}）——
 * 多态化不该顺手改变既有图表的默认外观。
 *
 * <p>不建 {@code .json}：没有任何 {@code @FormField}，没有可描述的表单项。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/25
 */
public class DecimalBoundOff extends AbstractDecimalBound {

    private static final long serialVersionUID = 1L;

    @TISExtension
    public static class DefaultDescriptor extends BasicDescriptor implements DescriptorUseableShortComment {
        @Override
        public String getDisplayName() {
            return SWITCH_OFF;
        }

        @Override
        public String shortComment() {
            return "自动计算";
        }
    }
}
