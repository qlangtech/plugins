package com.qlangtech.tis.plugin.ontology.workshop.model.section;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;

/**
 * 标签页布局：同一位置切换显示的子分区。
 */
public class TabsLayout extends SectionLayout {

    @FormField(ordinal = 0, type = FormFieldType.ENUM)
    public Boolean showTabBar = true;

    @FormField(ordinal = 1, type = FormFieldType.ENUM)
    public TabsLayout.TabPosition tabPosition = TabsLayout.TabPosition.TOP;

    @TISExtension
    public static class DefaultDescriptor extends BasicDescriptor
            implements DescriptorUseableShortComment {
        @Override
        public String shortComment() {
            return "标签页布局";
        }

        @Override
        public String getDisplayName() {
            return "Tabs Layout";
        }
    }

    /**
     * Tab 栏位置。
     *
     * <p>⚠️ 枚举常量名会以<b>大写</b>下发到前端（{@code TOP} / {@code LEFT} / …），而
     * {@code workshop-section.component.html} 把它直接喂给 {@code nzTabPosition}，
     * 后者要的是小写。前端在 {@code editor-config-panel} 的回填/写回转换里做了归一化，
     * 改这里的常量名要同步改那边。
     */
    public enum TabPosition implements DescriptorUseableShortComment {
        TOP("顶部"),
        LEFT("左侧"),
        RIGHT("右侧"),
        BOTTOM("底部");

        private final String comment;

        TabPosition(String comment) {
            this.comment = comment;
        }

        @Override
        public String shortComment() {
            return comment;
        }
    }
}
