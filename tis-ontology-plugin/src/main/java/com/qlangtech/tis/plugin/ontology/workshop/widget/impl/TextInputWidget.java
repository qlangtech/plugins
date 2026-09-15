package com.qlangtech.tis.plugin.ontology.workshop.widget.impl;

import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.workshop.widget.IWorkshopWidget;
import com.qlangtech.tis.plugin.workshop.widget.WorkshopWidget;

/**
 * P0 Widget：文本输入框（Text Input）
 * <p>
 * 文本输入组件，支持前缀/后缀图标、输入类型、placeholder 配置，输出变量值。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/12
 */
public class TextInputWidget extends WorkshopWidget {

    public static final String KEY_INITIAL_VALUE_VAR = "initialValueVar";
    public static final String KEY_VALUE_VAR = "valueVar";

    /** 初始值来源：当前模块中的字符串变量，可选（不设则输入框起始为空） */
    @FormField(type = FormFieldType.SELECTABLE, ordinal = 10, advance = false)
    public String initialValueVar;

    /** 输入值写回：用户输入内容写入该变量，供下游组件消费 */
    @FormField(type = FormFieldType.SELECTABLE, ordinal = 11, advance = false)
    public String valueVar;

    @TISExtension
    public static class DescriptorImpl extends BaseWidgetDescriptor<IWorkshopWidget> {

        public DescriptorImpl() {
            super();
            this.registerSelectOptions(KEY_INITIAL_VALUE_VAR, WidgetOptionHelper::getStringVariableOptions);
            this.registerSelectOptions(KEY_VALUE_VAR, WidgetOptionHelper::getStringVariableOptions);
        }

        @Override
        public String getDisplayName() {
            return "Text Input";
        }

        @Override
        public WorkShopWidgetType getWidgetType() {
            return WorkShopWidgetType.TEXT_INPUT;
        }
    }
}