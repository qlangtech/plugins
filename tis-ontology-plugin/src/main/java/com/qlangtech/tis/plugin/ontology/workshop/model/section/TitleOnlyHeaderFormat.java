package com.qlangtech.tis.plugin.ontology.workshop.model.section;

import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;

/**
 * 仅标题 —— 渲染 section header，只显示标题文本（{@code section.name}）。
 *
 * <p>零字段：标题文本来自 section 实体自身的 {@code name} 字段，不需要在 header 格式上
 * 重复配置，因此本类<b>没有</b>对应的 {@code .json}。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/22
 */
public class TitleOnlyHeaderFormat extends HeaderFormat {

    private static final long serialVersionUID = 1L;

    @TISExtension
    public static class DefaultDescriptor extends BasicDescriptor implements DescriptorUseableShortComment {

        @Override
        public String getDisplayName() {
            return "TITLE";
        }

        @Override
        public String shortComment() {
            return "仅标题";
        }
    }
}