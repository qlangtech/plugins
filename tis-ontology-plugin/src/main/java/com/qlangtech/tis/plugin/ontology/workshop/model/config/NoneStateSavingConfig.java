package com.qlangtech.tis.plugin.ontology.workshop.model.config;

import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;

/**
 * 「不启用」形态的变量状态保存配置：变量值只活在当前会话。
 *
 * <p>零字段、无 {@code .json}，理由见 {@link NoneInterfaceConfig}。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/16
 */
public class NoneStateSavingConfig extends VariableStateSavingConfig {

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
