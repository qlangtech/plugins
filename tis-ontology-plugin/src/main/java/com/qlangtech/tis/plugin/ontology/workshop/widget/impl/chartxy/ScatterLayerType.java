package com.qlangtech.tis.plugin.ontology.workshop.widget.impl.chartxy;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;

/**
 * 散点图画法：零字段。
 *
 * <p>散点图既不支持面积选项（没有折线可填），也不支持数值标签（点密集时会糊在一起），
 * 因此没有任何专有配置。这也正是把它做成独立子类、而不是「枚举 + 一堆只在部分取值下
 * 生效的字段」的原因：零字段本身就是一个可断言的契约。
 *
 * <p>不建 {@code .json}：没有任何 {@code @FormField}。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/24
 */
public class ScatterLayerType extends AbstractLayerType {

    private static final long serialVersionUID = 1L;

    @TISExtension
    public static class DefaultDescriptor extends BasicDescriptor implements DescriptorUseableShortComment {
        @Override
        public String getDisplayName() {
            return "Scatter";
        }

        @Override
        public EndType getEndType() {
            return EndType.Scatter;
        }

        @Override
        public String shortComment() {
            return "散点图";
        }
    }
}
