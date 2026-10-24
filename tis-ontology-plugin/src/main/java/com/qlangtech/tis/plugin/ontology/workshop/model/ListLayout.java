package com.qlangtech.tis.plugin.ontology.workshop.model;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;

/**
 * 列表布局：指标上下排成列表，可配标签/数值模板。
 *
 * @author 百岁 (baisui@qlangtech.com)
 */
public class ListLayout extends LayoutStyle {

    private static final long serialVersionUID = 1L;

    @FormField(ordinal = 0, type = FormFieldType.ENUM)
    public Template template = Template.SIDE_BY_SIDE;

    @TISExtension
    public static class DefaultDescriptor extends BasicDescriptor implements DescriptorUseableShortComment {

        @Override
        public String getDisplayName() {
            return "List Layout";
        }

        @Override
        public String shortComment() {
            return "列表：指标上下排列";
        }
    }
}
