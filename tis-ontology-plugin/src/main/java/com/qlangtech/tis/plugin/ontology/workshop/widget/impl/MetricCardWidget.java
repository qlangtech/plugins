package com.qlangtech.tis.plugin.ontology.workshop.widget.impl;

import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.workshop.widget.IWorkshopWidget;
import com.qlangtech.tis.plugin.workshop.widget.WorkshopWidget;

/**
 * P0 Widget：指标卡（Metric Card）
 * <p>
 * 以统计卡片形式展示单个数值指标，支持标题、前缀、后缀和颜色定制。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/12
 */
public class MetricCardWidget extends WorkshopWidget {

    public static final String KEY_VALUE_VAR = "valueVar";

    /** 指标取值来源：当前模块中的数值变量（options 由 Descriptor 动态供给） */
    @FormField(type = FormFieldType.SELECTABLE, ordinal = 10, advance = false)
    public String valueVar;

    @TISExtension
    public static class DescriptorImpl extends BaseWidgetDescriptor<IWorkshopWidget> {

        public DescriptorImpl() {
            super();
            this.registerSelectOptions(KEY_VALUE_VAR, WidgetOptionHelper::getNumericVariableOptions);
        }

        @Override
        public String getDisplayName() {
            return "Metric Card";
        }

        @Override
        public WorkShopWidgetType getWidgetType() {
            return WorkShopWidgetType.METRIC_CARD;
        }
    }
}