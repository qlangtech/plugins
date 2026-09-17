package com.qlangtech.tis.plugin.ontology.workshop.model;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.WidgetOptionHelper;

/**
 * 对象集变量输入（Object Set）
 *
 * <p>图层的数据来自当前 Workshop Module 中的一个对象集变量。本类只管一件事：
 * <b>绑定到哪个对象集变量</b>；聚合方式落在 {@link SeriesConfig}，分组键落在
 * {@link ChartLayer#xAxisProperty}。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/15
 * @see LayerDataInput
 */
public class ObjectSetDataInput extends LayerDataInput {

    private static final long serialVersionUID = 1L;

    public static final String KEY_DATA_SOURCE_VAR = "dataSourceVar";

    /**
     * 数据源变量名；选项由 {@code DescriptorImpl} 动态供给当前 Module 的对象集变量。
     *
     * <p>选项供给此前挂在 {@link ChartLayer.DescriptorImpl} 上，与输入方式的判别符脱钩
     * —— 选了「时间序列集」也照样列对象集变量。多态拆分后它归本子类专有。
     */
    @FormField(ordinal = 10, type = FormFieldType.SELECTABLE, validate = {Validator.require})
    public String dataSourceVar;

    @TISExtension
    public static class DefaultDescriptor extends BasicDescriptor implements DescriptorUseableShortComment {

        public DefaultDescriptor() {
            super();
            // 数据源变量选项：当前 Module 中的对象集变量
            this.registerSelectOptions(KEY_DATA_SOURCE_VAR, WidgetOptionHelper::getObjectSetVariableOptions);
        }

        @Override
        public String getDisplayName() {
            return "Object Set";
        }

        @Override
        public String shortComment() {
            return "对象集变量";
        }
    }
}