package com.qlangtech.tis.plugin.ontology.workshop.widget.component;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;

/**
 * 单选：一列互斥的选项，默认算子 {@code equals}。
 *
 * <p>零字段 —— 候选项来源在父类 {@link AbstractSelectComponent#optionsSource} 上。
 * 也因此本类<b>不</b>需要自己的 {@code .json}（父类那份会被合并进来）。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/11/2
 */
public class SingleSelectComponent extends AbstractSelectComponent {

    private static final long serialVersionUID = 1L;

    @TISExtension
    public static class DefaultDescriptor extends SelectDescriptor implements DescriptorUseableShortComment {

        /**
         * 线格式：前端派生 kebab token {@code single-select}。
         */
        @Override
        public String getDisplayName() {
            return "single-select";
        }

        @Override
        public String shortComment() {
            return "单选：从候选项里选一个";
        }
    }
}
