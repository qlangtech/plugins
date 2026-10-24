package com.qlangtech.tis.plugin.ontology.workshop.model.section;

import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;

/**
 * 无标题 —— 不渲染 section header。
 *
 * <p>零字段：既然不显示标题，就没有任何可配项，因此本类<b>没有</b>对应的 {@code .json}。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/22
 */
public class NoneHeaderFormat extends HeaderFormat {

    private static final long serialVersionUID = 1L;

    @TISExtension
    public static class DefaultDescriptor extends BasicDescriptor implements DescriptorUseableShortComment {

        @Override
        public String getDisplayName() {
            return SWITCH_OFF;
        }

        @Override
        public String shortComment() {
            return "无标题";
        }
    }
}