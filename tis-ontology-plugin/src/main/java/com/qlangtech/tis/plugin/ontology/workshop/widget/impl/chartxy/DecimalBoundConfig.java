package com.qlangtech.tis.plugin.ontology.workshop.widget.impl.chartxy;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;

/**
 * 数值轴边界的「手工指定」实现。
 *
 * <p>{@link #value} 带 {@code Validator.require}：既然用户显式选择了「手工指定」，就该
 * 给出边界值 —— 留空与「自动计算」没有区别，必填把两种状态区分开（与
 * {@link AxisTitleConfig#titleOverride} 同一理由）。
 *
 * <p>不限制正负、上界与下界也不在此比较大小：同一个类同时服务上界与下界两个宿主字段，
 * 「下界不得大于上界」是<b>两字段之间的</b>约束，只能由宿主
 * {@code ContinuousAxisConfig} 在保存期校验。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/25
 */
public class DecimalBoundConfig extends AbstractDecimalBound {

    private static final long serialVersionUID = 1L;

    /**
     * 手工指定的边界值。
     */
    @FormField(ordinal = 0, type = FormFieldType.DECIMAL_NUMBER, validate = {Validator.require})
    public Double value;

    @TISExtension
    public static class DefaultDescriptor extends BasicDescriptor implements DescriptorUseableShortComment {
        @Override
        public String getDisplayName() {
            return "Decimal Bound";
        }

        @Override
        public String shortComment() {
            return "手工指定边界";
        }
    }
}
