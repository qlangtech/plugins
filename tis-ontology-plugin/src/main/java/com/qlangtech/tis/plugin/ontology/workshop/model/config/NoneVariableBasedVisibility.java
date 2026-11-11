package com.qlangtech.tis.plugin.ontology.workshop.model.config;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;

/**
 * 基于变量的可见性的「关闭」实现：零字段，浮层恒显示。
 *
 * <p>不建 {@code .json}：没有任何 {@code @FormField}，没有可描述的表单项。
 *
 * <p>类名以 {@code None} 开头是硬约定：测试辅助
 * {@code TisPluginFormTestSupport#offSubclassOf} 按该前缀识别 off 子类。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/11/11
 */
public class NoneVariableBasedVisibility extends VariableBasedVisibilityConfig {

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
