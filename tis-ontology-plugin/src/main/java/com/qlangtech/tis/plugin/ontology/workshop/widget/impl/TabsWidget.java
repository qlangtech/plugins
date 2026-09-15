package com.qlangtech.tis.plugin.ontology.workshop.widget.impl;

import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.workshop.widget.IWorkshopWidget;
import com.qlangtech.tis.plugin.workshop.widget.WorkshopWidget;

/**
 * P0 Widget：标签页（Tabs）
 * <p>
 * 标签页切换组件，支持 Header Tabs（切换 Page）和 Section Tabs（触发事件）两种模式。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/12
 */
public class TabsWidget extends WorkshopWidget {

    @TISExtension
    public static class DescriptorImpl extends BaseWidgetDescriptor<IWorkshopWidget> {

        @Override
        public String getDisplayName() {
            return "Tabs";
        }

        @Override
        public WorkShopWidgetType getWidgetType() {
            return WorkShopWidgetType.TABS;
        }
    }
}