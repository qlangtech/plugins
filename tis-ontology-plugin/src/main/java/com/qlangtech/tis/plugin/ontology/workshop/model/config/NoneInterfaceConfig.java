package com.qlangtech.tis.plugin.ontology.workshop.model.config;

import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;

/**
 * 「不启用」形态的变量接口配置。
 *
 * <p>零字段是刻意的：既然不启用，就没有任何可配项，因此本类<b>没有</b>对应的
 * {@code .json}（没有 {@code @FormField} 字段就无处可写），与
 * {@code model.definition.ExternalInputConfig} 同样处理。
 *
 * <p>{@link #getDisplayName()} 返回 {@link Descriptor#SWITCH_OFF}（即 {@code "off"}）
 * 是这套开关机制的<b>全部</b>：前端 {@code tis.plugin.ts} 拿
 * {@code WorkshopVariable.json} 里该字段的 {@code dftVal} 去逐项比对各个子类的
 * {@code displayName} 来选默认项。因此这里改成别的字符串，默认项就悄悄丢了。
 * 先例见 {@code proxy.impl.ProxyAuthOff.OffDesc}。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/16
 */
public class NoneInterfaceConfig extends VariableInterfaceConfig {

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
