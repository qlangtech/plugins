package com.qlangtech.tis.plugin.ontology.workshop.model.section;

import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;

/**
 * 标题和描述 —— 在标题下方额外渲染一行描述文本。
 *
 * <p>描述文本由本类的 {@link #description} 字段承载，表单侧只有选中本子类时才出现该输入框。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/22
 */
public class TitleWithDescriptionHeaderFormat extends HeaderFormat {

    private static final long serialVersionUID = 1L;

    @FormField(ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String description;

    @TISExtension
    public static class DefaultDescriptor extends BasicDescriptor implements DescriptorUseableShortComment {

        @Override
        public String getDisplayName() {
            return "TITLE_WITH_DESCRIPTION";
        }

        @Override
        public String shortComment() {
            return "标题和描述";
        }
    }
}