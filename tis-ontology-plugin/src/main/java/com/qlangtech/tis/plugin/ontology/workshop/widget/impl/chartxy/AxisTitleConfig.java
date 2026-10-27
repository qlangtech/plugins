package com.qlangtech.tis.plugin.ontology.workshop.widget.impl.chartxy;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;

/**
 * 轴标题的「显示」实现。
 *
 * <p>{@link #titleOverride} 带 {@code Validator.require}：既然用户显式选择了「显示轴标题」，
 * 再留空让后端自动生成一个标题，就与「选 off」没有区别了 —— 必填把两种状态区分开。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/24
 */
public class AxisTitleConfig extends AbstractAxisTitleConfig {

    private static final long serialVersionUID = 1L;

    /**
     * 轴标题覆写文本 —— 显示轴标题时必填。
     */
    @FormField(ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String titleOverride;

    @TISExtension
    public static class DefaultDescriptor extends BasicDescriptor implements DescriptorUseableShortComment {
        @Override
        public String getDisplayName() {
            return "Axis Title";
        }

        @Override
        public String shortComment() {
            return "显示轴标题";
        }
    }
}
