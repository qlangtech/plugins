package com.qlangtech.tis.plugin.ontology.workshop.widget.impl;

import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.workshop.widget.IWorkshopWidget;
import com.qlangtech.tis.plugin.workshop.widget.WorkshopWidget;

/**
 * P0 Widget：按钮组（Button Group）
 * <p>
 * 按钮组容器，支持水平/垂直布局、多种按钮类型、条件可见性、
 * 以及事件/动作/链接/导出四种点击行为。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/12
 */
public class ButtonGroupWidget extends WorkshopWidget {

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