package com.qlangtech.tis.plugin.ontology.workshop.widget.impl;

import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.workshop.widget.IWorkshopWidget;
import com.qlangtech.tis.plugin.workshop.widget.WorkshopWidget;

/**
 * P0 Widget：过滤列表（Filter List）
 * <p>
 * 组合多种过滤器组件（关键词、单选、多选、日期选择），统一输出过滤条件。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/12
 */
public class FilterListWidget extends WorkshopWidget {

    public static final String KEY_FILTER_OUTPUT_VAR = "filterOutputVar";

    /** 过滤结果写回：组合出的过滤条件写入该「对象集合过滤器」变量，供下游组件作为过滤源 */
    @FormField(type = FormFieldType.SELECTABLE, ordinal = 10, advance = false)
    public String filterOutputVar;

    @TISExtension
    public static class DescriptorImpl extends BaseWidgetDescriptor<IWorkshopWidget> {

        public DescriptorImpl() {
            super();
            this.registerSelectOptions(KEY_FILTER_OUTPUT_VAR,
                    WidgetOptionHelper::getObjectSetFilterVariableOptions);
        }

        @Override
        public String getDisplayName() {
            return "Filter List";
        }

        @Override
        public WorkShopWidgetType getWidgetType() {
            return WorkShopWidgetType.FILTER_LIST;
        }
    }
}