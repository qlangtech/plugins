package com.qlangtech.tis.plugin.ontology.workshop.widget.component;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;

/**
 * 关键词输入框：一个自由文本输入，默认算子 {@code contains}。
 *
 * <p>零字段 —— 见 {@link FilterComponent} 的多态理由。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/11/2
 */
public class KeywordComponent extends FilterComponent {

    private static final long serialVersionUID = 1L;

    @TISExtension
    public static class DefaultDescriptor extends BasicDescriptor implements DescriptorUseableShortComment {

        /**
         * 线格式：前端 {@code resolveFilterComponent} 按 {@code resolveImplKind(impl, "Component")}
         * 从本值派生 kebab token（{@code keyword}）。
         */
        @Override
        public String getDisplayName() {
            return "keyword";
        }

        @Override
        public String shortComment() {
            return "关键词：模糊匹配输入文本";
        }
    }
}
