package com.qlangtech.tis.plugin.ontology.workshop.model.section;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;

/**
 * 工具栏布局：单行紧凑排列，适合按钮与筛选器。
 *
 * <p>与 {@link RowsLayout} 同理，无可配参数但必须自成一类 —— 布局身份由 Java 类型承担。
 * 另注意前端把本布局与 {@code loop} 一起归入
 * {@code editor-actions.service.ts} 的 {@code LAYOUTS_WITHOUT_CHILDREN}：
 * 挂上子分区在视觉上无处安放，切换时会提示用户。
 */
public class ToolbarLayout extends SectionLayout {

    @TISExtension
    public static class DefaultDescriptor extends BasicDescriptor
            implements DescriptorUseableShortComment {
        @Override
        public String shortComment() {
            return "工具栏布局";
        }

        @Override
        public String getDisplayName() {
            return "Toolbar Layout";
        }
    }
}
