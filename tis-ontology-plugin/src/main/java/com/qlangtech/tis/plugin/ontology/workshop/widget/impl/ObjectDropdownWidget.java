package com.qlangtech.tis.plugin.ontology.workshop.widget.impl;

import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.workshop.widget.IWorkshopWidget;
import com.qlangtech.tis.plugin.workshop.widget.WorkshopWidget;

/**
 * P0 Widget：对象下拉选择（Object Dropdown）
 * <p>
 * 下拉选择器，从对象集合中选择一个对象，支持搜索过滤和输出变量绑定。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/12
 */
public class ObjectDropdownWidget extends WorkshopWidget {

    public static final String KEY_OBJECT_SET_VAR = "objectSetVar";
    public static final String KEY_SELECTED_OBJECT_VAR = "selectedObjectVar";

    /** 候选对象来源：当前模块中的对象集合变量（options 由 Descriptor 动态供给） */
    @FormField(type = FormFieldType.SELECTABLE, ordinal = 10, advance = false)
    public String objectSetVar;

    /** 选中项写回：下拉选中一个对象后写入该变量，供下游组件消费 */
    @FormField(type = FormFieldType.SELECTABLE, ordinal = 11, advance = false)
    public String selectedObjectVar;

    @TISExtension
    public static class DescriptorImpl extends BaseWidgetDescriptor<IWorkshopWidget> {

        public DescriptorImpl() {
            super();
            this.registerSelectOptions(KEY_OBJECT_SET_VAR, WidgetOptionHelper::getObjectSetVariableOptions);
            this.registerSelectOptions(KEY_SELECTED_OBJECT_VAR, WidgetOptionHelper::getObjectSetVariableOptions);
        }

        @Override
        public String getDisplayName() {
            return "Object Dropdown";
        }

        @Override
        public WorkShopWidgetType getWidgetType() {
            return WorkShopWidgetType.OBJECT_DROPDOWN;
        }
    }
}