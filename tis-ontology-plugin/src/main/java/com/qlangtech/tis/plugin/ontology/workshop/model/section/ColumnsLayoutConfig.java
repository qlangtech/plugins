package com.qlangtech.tis.plugin.ontology.workshop.model.section;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;

/**
 * Columns 布局配置
 */
public class ColumnsLayoutConfig extends SectionLayoutConfig {

    @FormField(ordinal = 0, type = FormFieldType.INT_NUMBER, validate = {Validator.require})
    public Integer columnCount = 2;

    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT)
    public String columnWidths; // e.g., "1fr 2fr 1fr"

    @TISExtension
    public static class DefaultDescriptor extends BasicDescriptor
            implements DescriptorUseableShortComment {
        @Override
        public String shortComment() {
            return "列布局配置";
        }

        @Override
        public String getDisplayName() {
            return "Columns Layout";
        }
    }
}
