package com.qlangtech.tis.plugin.ontology.workshop.model.section;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;

import java.io.Serializable;

/**
 * Section 样式配置
 */
public class SectionStyleConfig implements Describable<SectionStyleConfig>, Serializable {

    @FormField(ordinal = 0, type = FormFieldType.SELECTABLE)
    public SectionStyleConfig.HeaderFormat headerFormat = SectionStyleConfig.HeaderFormat.NONE;

    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT)
    public String backgroundColor;

    @FormField(ordinal = 2, type = FormFieldType.SELECTABLE)
    public SectionStyleConfig.BorderStyle borderStyle = SectionStyleConfig.BorderStyle.NONE;

    @FormField(ordinal = 3, type = FormFieldType.INT_NUMBER)
    public Integer padding = 16;

    @TISExtension
    public static class DefaultDescriptor extends Descriptor<SectionStyleConfig> {
        @Override
        public String getDisplayName() {
            return "Section Style Config";
        }
    }

    public enum HeaderFormat implements DescriptorUseableShortComment {
        NONE("无标题"),
        TITLE("仅标题"),
        TITLE_WITH_DESCRIPTION("标题和描述");

        private final String comment;

        HeaderFormat(String comment) {
            this.comment = comment;
        }

        @Override
        public String shortComment() {
            return comment;
        }
    }

    public enum BorderStyle implements DescriptorUseableShortComment {
        NONE("无边框"),
        SOLID("实线"),
        DASHED("虚线"),
        ROUNDED("圆角");

        private final String comment;

        BorderStyle(String comment) {
            this.comment = comment;
        }

        @Override
        public String shortComment() {
            return comment;
        }
    }
}
