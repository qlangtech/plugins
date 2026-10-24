package com.qlangtech.tis.plugin.ontology.workshop.model;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;

/**
 * 条件可见性的「关闭」实现：零字段，宿主恒显示。
 *
 * <p>不建 {@code .json}：没有任何 {@code @FormField}，没有可描述的表单项。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/22
 */
public class ConditionalVisibilityOff extends ConditionalVisibilityConfig {

    private static final long serialVersionUID = 1L;

    @TISExtension
    public static class DefaultDescriptor extends BasicDescriptor implements DescriptorUseableShortComment {
        @Override
        public String getDisplayName() {
            return SWITCH_OFF;
        }

        @Override
        public String shortComment() {
            return "不启用";
        }
    }
}
