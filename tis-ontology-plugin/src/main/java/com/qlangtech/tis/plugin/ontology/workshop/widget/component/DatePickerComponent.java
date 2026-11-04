package com.qlangtech.tis.plugin.ontology.workshop.widget.component;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;

/**
 * 日期选择器：选出一个日期，默认算子 {@code equals}。
 *
 * <p>零字段 —— 见 {@link FilterComponent} 的多态理由。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/11/2
 */
public class DatePickerComponent extends FilterComponent {

    private static final long serialVersionUID = 1L;

    @TISExtension
    public static class DefaultDescriptor extends BasicDescriptor implements DescriptorUseableShortComment {

        /**
         * 线格式：前端派生 kebab token {@code date-picker}。
         */
        @Override
        public String getDisplayName() {
            return "date-picker";
        }

        @Override
        public String shortComment() {
            return "日期选择：精确到某一天";
        }
    }
}
