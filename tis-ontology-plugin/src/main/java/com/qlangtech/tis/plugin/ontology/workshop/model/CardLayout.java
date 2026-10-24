package com.qlangtech.tis.plugin.ontology.workshop.model;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;

/**
 * 卡片布局：每个指标一张卡片。可配排列方向与标签/数值模板。
 *
 * @author 百岁 (baisui@qlangtech.com)
 */
public class CardLayout extends LayoutStyle {

    private static final long serialVersionUID = 1L;

    @FormField(ordinal = 0, type = FormFieldType.ENUM)
    public Direction direction = Direction.HORIZONTAL;

    @FormField(ordinal = 1, type = FormFieldType.ENUM)
    public Template template = Template.STACKED;

    @TISExtension
    public static class DefaultDescriptor extends BasicDescriptor implements DescriptorUseableShortComment {

        @Override
        public String getDisplayName() {
            return "Card Layout";
        }

        @Override
        public String shortComment() {
            return "卡片：每个指标一张卡片";
        }
    }
}
