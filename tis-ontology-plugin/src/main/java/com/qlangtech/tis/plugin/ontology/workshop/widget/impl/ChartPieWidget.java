package com.qlangtech.tis.plugin.ontology.workshop.widget.impl;

import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.workshop.widget.IWorkshopWidget;
import com.qlangtech.tis.plugin.workshop.widget.WorkshopWidget;

/**
 * P0 Widget：饼图（Chart Pie）
 * <p>
 * 饼图 / 环形图，支持标签和数值属性绑定，内置默认色板。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/12
 */
public class ChartPieWidget extends WorkshopWidget {

    public static final String KEY_DATA_SOURCE_VAR = "dataSourceVar";

    /** 数据来源：当前模块中的对象集合变量（options 由 Descriptor 动态供给） */
    @FormField(type = FormFieldType.SELECTABLE, ordinal = 10, advance = false)
    public String dataSourceVar;

    @TISExtension
    public static class DescriptorImpl extends BaseWidgetDescriptor<IWorkshopWidget> {

        public DescriptorImpl() {
            super();
            this.registerSelectOptions(KEY_DATA_SOURCE_VAR, WidgetOptionHelper::getObjectSetVariableOptions);
        }

        @Override
        public String getDisplayName() {
            return "Chart Pie";
        }

        @Override
        public WorkShopWidgetType getWidgetType() {
            return WorkShopWidgetType.CHART_PIE;
        }
    }
}