package com.qlangtech.tis.plugin.ontology.workshop.model.section;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;

/**
 * Tabs 布局配置
 */
public class TabsLayoutConfig extends SectionLayoutConfig {

    @FormField(ordinal = 0, type = FormFieldType.ENUM)
    public Boolean showTabBar = true;

    @FormField(ordinal = 1, type = FormFieldType.SELECTABLE)
    public TabsLayoutConfig.TabPosition tabPosition = TabsLayoutConfig.TabPosition.TOP;

    @TISExtension
    public static class DefaultDescriptor extends BasicDescriptor
            implements DescriptorUseableShortComment {
        @Override
        public String shortComment() {
            return "标签页布局配置";
        }

        @Override
        public String getDisplayName() {
            return "Tabs Layout";
        }
    }

    enum TabPosition implements DescriptorUseableShortComment {
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
