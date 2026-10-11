package com.qlangtech.tis.plugin.ontology.workshop.model.section;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;

/**
 * 循环布局：按对象集重复渲染同一分区。
 */
public class LoopLayout extends SectionLayout {

    @FormField(ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String iteratorVariable; // 变量 ID

    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT)
    public String itemVariable; // 迭代项变量名

    @TISExtension
    public static class DefaultDescriptor extends BasicDescriptor
            implements DescriptorUseableShortComment {
        @Override
        public String shortComment() {
            return "循环布局";
        }

        @Override
        public String getDisplayName() {
            return "Loop Layout";
        }
    }
}
