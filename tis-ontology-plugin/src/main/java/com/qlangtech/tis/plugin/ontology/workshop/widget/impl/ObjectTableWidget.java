package com.qlangtech.tis.plugin.ontology.workshop.widget.impl;

import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.workshop.widget.IWorkshopWidget;
import com.qlangtech.tis.plugin.workshop.widget.WidgetColumnConfig;
import com.qlangtech.tis.plugin.workshop.widget.WorkshopWidget;

import java.util.List;
import java.util.Map;

/**
 * P1 Widget：对象列表表格（Object Table）
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/9
 */
public class ObjectTableWidget extends WorkshopWidget {

    public static final String KEY_OBJECT_SET_VAR = "objectSetVar";
    public static final String KEY_ACTIVE_OBJECT_VAR = "activeObjectVar";
    public static final String KEY_SELECTED_OBJECTS_VAR = "selectedObjectsVar";

    /**
     * 对象集输入变量（options 动态供给）
     */
    @FormField(type = FormFieldType.SELECTABLE, ordinal = 20, advance = false, validate = {Validator.require})
    public String objectSetVar;

    /**
     * 每页显示条数
     */
    @FormField(type = FormFieldType.INT_NUMBER, ordinal = 21, advance = false, validate = {Validator.require})
    public Integer pageSize;

    /**
     * 列定义列表（由 SubForm 子表单结构化管理）
     */
    @FormField(type = FormFieldType.MULTI_SELECTABLE, ordinal = 22, advance = false, validate = {Validator.require})
    public List<WidgetColumnConfig> columns;

    /** 当前行写回：点击某行后把该行对象写入此变量，供下游组件消费 */
    @FormField(type = FormFieldType.SELECTABLE, ordinal = 23, advance = false)
    public String activeObjectVar;

    /** 多选集合写回：勾选集合变化后把选中行数组写入此变量，供下游组件消费 */
    @FormField(type = FormFieldType.SELECTABLE, ordinal = 24, advance = false)
    public String selectedObjectsVar;

    @TISExtension
    public static class DescriptorImpl extends BaseWidgetDescriptor<IWorkshopWidget> {

        public DescriptorImpl() {
            super();
            this.registerSelectOptions(KEY_OBJECT_SET_VAR, WidgetOptionHelper::getObjectSetVariableOptions);
            this.registerSelectOptions(KEY_ACTIVE_OBJECT_VAR, WidgetOptionHelper::getObjectSetVariableOptions);
            this.registerSelectOptions(KEY_SELECTED_OBJECTS_VAR, WidgetOptionHelper::getObjectSetVariableOptions);
        }

        @Override
        public String getDisplayName() {
            return "Object Table";
        }

        @Override
        public WorkShopWidgetType getWidgetType() {
            return WorkShopWidgetType.OBJECT_TABLE;
        }

        @Override
        protected Map<String, Object> appendExtractProps(Map<String, Object> props) {
            props.put("defaultSize", Map.of("height", 320));
            return props;
        }
    }
}