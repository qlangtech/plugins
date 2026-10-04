package com.qlangtech.tis.plugin.ontology.workshop.widget.impl;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.workshop.widget.WidgetButtonConfig;
import com.qlangtech.tis.plugin.ontology.workshop.widget.CompactDisplayWidget;
import com.qlangtech.tis.plugin.workshop.widget.IWorkshopWidget;

import java.util.List;

/**
 * P0 Widget：按钮组（Button Group）
 * <p>
 * 按钮组容器，支持水平/垂直布局、多种按钮类型、条件可见性，
 * 以及事件 / 本体动作 / 链接 / 导出四种点击行为。
 * <p>
 * 每个按钮的完整形状（文案、形态、图标、条件可见性、点击行为）由
 * {@link WidgetButtonConfig} 承担；点击行为本身是 {@code WidgetClickAction} 多态，
 * 对应前端的四种分支。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/12
 */
public class ButtonGroupWidget extends CompactDisplayWidget {

    public static final String KEY_BUTTONS = "buttons";

    @FormField(type = FormFieldType.ENUM, ordinal = 10, advance = false)
    public GroupLayout layout = GroupLayout.horizontal;

    @FormField(type = FormFieldType.MULTI_DESCRIBLE_PLUGIN, desClazz = WidgetButtonConfig.class,
            ordinal = 11, advance = false, validate = {Validator.require})
    public List<WidgetButtonConfig> buttons;

    /** 按钮排列方向 */
    public enum GroupLayout implements DescriptorUseableShortComment {
        horizontal("水平排列"), vertical("垂直排列");
        public final String label;

        GroupLayout(String label) {
            this.label = label;
        }

        @Override
        public String shortComment() {
            return this.label;
        }
    }

    @TISExtension
    public static class DescriptorImpl extends BaseWidgetDescriptor<IWorkshopWidget> {

        @Override
        public String getDisplayName() {
            return "Button Group";
        }

        @Override
        public WorkShopWidgetType getWidgetType() {
            return WorkShopWidgetType.BUTTON_GROUP;
        }
    }
}
