package com.qlangtech.tis.plugin.ontology.workshop.model.header;

import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;

/**
 * 「不可折叠」形态的 header 折叠配置。
 *
 * <p>零字段是刻意的：既然不可折叠，就没有任何可配项，因此本类<b>没有</b>对应的
 * {@code .json}（没有 {@code @FormField} 字段就无处可写），与
 * {@code model.config.NoneInterfaceConfig} 同样处理。
 *
 * <p>{@link #getDisplayName()} 返回 {@link Descriptor#SWITCH_OFF}（即 {@code "off"}）
 * 是这套开关机制的<b>全部</b>：前端 {@code tis.plugin.ts} 拿
 * {@code WorkshopHeader.json} 里该字段的 {@code dftVal} 去逐项比对各个子类的
 * {@code displayName} 来选默认项。因此这里改成别的字符串，默认项就悄悄丢了 ——
 * 这也正是 {@code WorkshopHeader.json} 里 {@code collapseConfig} 写着
 * {@code "dftVal": "off"} 的原因，两侧字符串必须逐字相同。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/20
 */
public class NoneCollapseConfig extends CollapseConfig {

    private static final long serialVersionUID = 1L;

    @TISExtension
    public static class DefaultDescriptor extends BasicDescriptor implements DescriptorUseableShortComment {

        @Override
        public String getDisplayName() {
            return SWITCH_OFF;
        }

        @Override
        public String shortComment() {
            return "不可折叠";
        }
    }
}
