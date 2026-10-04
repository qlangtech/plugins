package com.qlangtech.tis.plugin.ontology.workshop.widget.impl;

import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.workshop.widget.WidgetActionConfig;
import com.qlangtech.tis.plugin.ontology.workshop.widget.CompactDisplayWidget;
import com.qlangtech.tis.plugin.workshop.widget.IWorkshopWidget;

/**
 * Action Button：Ontology Action 触发按钮
 * <p>
 * 触发 Ontology Action 执行，支持加载状态、条件禁用、成功/失败提示。
 * <p>
 * 按钮的文案 / 形态 / 图标 / 二次确认 / 触发的 Action 全部由 {@link WidgetActionConfig} 承载
 * —— 该模型是按钮与 Action 绑定的通用形状（ObjectList 的行内动作、按钮组的按钮项都用它），
 * 这里复用它而不是把五个字段平铺一遍，避免「按钮长什么样」出现多份互相漂移的定义。
 *
 * <a href="https://gitee.com/qlangtech/palantir-study/blob/master/foundry/workshop/detail/04-widgets/widgets-button-group.md">...</a>
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/12
 */
public class ActionButtonWidget extends CompactDisplayWidget {

    public static final String KEY_ACTION = "action";
    public static final String KEY_SELECTED_OBJECTS_VAR = "selectedObjectsVar";

    @FormField(ordinal = 10, advance = false, validate = {Validator.require})
    public WidgetActionConfig action;

    /** 执行 Action 时作为入参的对象来源：写入该变量的对象集合（options 由 Descriptor 动态供给） */
    @FormField(type = FormFieldType.SELECTABLE, ordinal = 11, advance = false)
    public String selectedObjectsVar;

    /** 未选中任何对象时是否禁用按钮 */
    @FormField(type = FormFieldType.ENUM, ordinal = 12, advance = false)
    public Boolean requiresSelection = false;

    @TISExtension
    public static class DescriptorImpl extends BaseWidgetDescriptor<IWorkshopWidget> {

        public DescriptorImpl() {
            super();
            this.registerSelectOptions(KEY_SELECTED_OBJECTS_VAR, WidgetOptionHelper::getObjectSetVariableOptions);
        }

        @Override
        public String getDisplayName() {
            return "Action Button";
        }

        @Override
        public WorkShopWidgetType getWidgetType() {
            return WorkShopWidgetType.ACTION_BUTTON;
        }
    }
}
