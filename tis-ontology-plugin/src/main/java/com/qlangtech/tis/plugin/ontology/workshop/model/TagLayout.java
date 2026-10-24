package com.qlangtech.tis.plugin.ontology.workshop.model;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;

/**
 * 标签布局：指标以紧凑的标签形式排列，适合一行展示多个小指标。
 *
 * @author 百岁 (baisui@qlangtech.com)
 */
public class TagLayout extends LayoutStyle {

    private static final long serialVersionUID = 1L;

    @FormField(ordinal = 0, type = FormFieldType.ENUM)
    public Direction direction = Direction.HORIZONTAL;

    @TISExtension
    public static class DefaultDescriptor extends BasicDescriptor implements DescriptorUseableShortComment {

        @Override
        public String getDisplayName() {
            return "Tag Layout";
        }

        @Override
        public String shortComment() {
            return "标签：紧凑排列多个指标";
        }
    }
}
