package com.qlangtech.tis.plugin.ontology.workshop.widget.component;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;

/**
 * 多选：可勾选多个选项，默认算子 {@code in}。
 *
 * <p>零字段 —— 候选项来源在父类 {@link AbstractSelectComponent#optionsSource} 上。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/11/2
 */
public class MultiSelectComponent extends AbstractSelectComponent {

    private static final long serialVersionUID = 1L;

    @TISExtension
    public static class DefaultDescriptor extends SelectDescriptor implements DescriptorUseableShortComment {

        /**
         * 线格式：前端派生 kebab token {@code multi-select}。
         */
        @Override
        public String getDisplayName() {
            return "multi-select";
        }

        @Override
        public String shortComment() {
            return "多选：可勾选多个候选项";
        }
    }
}
