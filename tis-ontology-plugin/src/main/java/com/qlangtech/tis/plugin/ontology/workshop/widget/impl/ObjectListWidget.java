package com.qlangtech.tis.plugin.ontology.workshop.widget.impl;

import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.workshop.widget.IWorkshopWidget;
import com.qlangtech.tis.plugin.workshop.widget.WorkshopWidget;

/**
 * P0 Widget：对象列表卡片（Object List）
 * <p>
 * 以卡片网格形式展示对象集合，支持响应式布局和点击选择输出变量。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/12
 */
public class ObjectListWidget extends WorkshopWidget {

    public static final String KEY_OBJECT_SET_VAR = "objectSetVar";
    public static final String KEY_ACTIVE_OBJECT_VAR = "activeObjectVar";

    /** 列表数据来源：当前模块中的对象集合变量（options 由 Descriptor 动态供给） */
    @FormField(type = FormFieldType.SELECTABLE, ordinal = 10, advance = false)
    public String objectSetVar;

    /** 当前选中项写回：点击卡片后写入该变量，供下游组件消费 */
    @FormField(type = FormFieldType.SELECTABLE, ordinal = 11, advance = false)
    public String activeObjectVar;

    @TISExtension
    public static class DescriptorImpl extends BaseWidgetDescriptor<IWorkshopWidget> {

        public DescriptorImpl() {
            super();
            this.registerSelectOptions(KEY_OBJECT_SET_VAR, WidgetOptionHelper::getObjectSetVariableOptions);
            this.registerSelectOptions(KEY_ACTIVE_OBJECT_VAR, WidgetOptionHelper::getObjectSetVariableOptions);
        }

        @Override
        public String getDisplayName() {
            return "Object List";
        }

        @Override
        public WorkShopWidgetType getWidgetType() {
            return WorkShopWidgetType.OBJECT_LIST;
        }
    }
}