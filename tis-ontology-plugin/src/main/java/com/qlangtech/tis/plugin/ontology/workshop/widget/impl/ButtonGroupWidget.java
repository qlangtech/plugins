package com.qlangtech.tis.plugin.ontology.workshop.widget.impl;

import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.workshop.widget.ButtonGroupType;
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

    /** 按钮组的呈现形态：内联平铺 / 折叠为下拉菜单 / 主按钮 + 菜单两段式（Palantir 的「Button type」）。
     *  改为 {@link ButtonGroupType} 多态承载 —— 仅内联平铺子类带 {@code layout} 载荷，另两个子类零字段。
     *  {@link Validator#require} 与 {@code dftVal=inline} 同 {@code ObjectTableWidget.pagenator} 口径：始终有值，防旧数据缺字段落 null。 */
    @FormField(ordinal = 9,validate = {Validator.require})
    public ButtonGroupType buttonType;

    @FormField(type = FormFieldType.MULTI_DESCRIBLE_PLUGIN,desClazz = WidgetButtonConfig.class,ordinal = 10,validate = {Validator.require})
    public List<WidgetButtonConfig> buttons;

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
