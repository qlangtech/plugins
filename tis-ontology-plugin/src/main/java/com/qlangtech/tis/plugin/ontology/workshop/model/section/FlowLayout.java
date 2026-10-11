package com.qlangtech.tis.plugin.ontology.workshop.model.section;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;

/**
 * 流式布局：按容器宽度自动换行排列。
 */
public class FlowLayout extends SectionLayout {

    /**
     * 容器最大高度（像素）。不填表示不限高。
     *
     * <p>这个字段原先只存在于前端 —— {@code workshop-section.component.html} 读
     * {@code section.layoutConfig?.maxHeight}，但后端从来没有对应字段，于是那条绑定
     * 永远取到 undefined。合并布局体系时把它补上，消掉这处前后端漂移。
     */
    @FormField(ordinal = 0, type = FormFieldType.INT_NUMBER)
    public Integer maxHeight;

    @TISExtension
    public static class DefaultDescriptor extends BasicDescriptor
            implements DescriptorUseableShortComment {
        @Override
        public String shortComment() {
            return "流式布局";
        }

        @Override
        public String getDisplayName() {
            return "Flow Layout";
        }
    }
}
