package com.qlangtech.tis.plugin.ontology.workshop.widget.impl;

import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.ontology.workshop.enums.VariableType;
import com.qlangtech.tis.plugin.workshop.widget.IWorkshopWidget;
import com.qlangtech.tis.plugin.workshop.widget.WorkshopWidget;

/**
 * P0 Widget：时间线（Timeline）
 * <p>
 * 以时间线形式展示事件序列，支持自定义颜色、图标、时间戳和内容。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/12
 */
public class TimelineWidget extends WorkshopWidget {

    public static final String KEY_DATA_SOURCE_VAR = "dataSourceVar";

    /** 事件来源：当前模块中的对象集合或时间序列集合变量（options 由 Descriptor 动态供给） */
    @FormField(type = FormFieldType.SELECTABLE, ordinal = 10, advance = false)
    public String dataSourceVar;

    @TISExtension
    public static class DescriptorImpl extends BaseWidgetDescriptor<IWorkshopWidget> {

        public DescriptorImpl() {
            super();
            // 同时接受两种类型：时间线的数据既可能是带时间属性的对象集合，也可能是时间序列集合。
            // 只认后者会让下拉在只建了对象集合的模块里恒为空。
            this.registerSelectOptions(KEY_DATA_SOURCE_VAR,
                    () -> WidgetOptionHelper.getVariableOptions(
                            VariableType.OBJECT_SET, VariableType.TIME_SERIES_SET));
        }

        @Override
        public String getDisplayName() {
            return "Timeline";
        }

        @Override
        public WorkShopWidgetType getWidgetType() {
            return WorkShopWidgetType.TIMELINE;
        }
    }
}