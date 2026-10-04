package com.qlangtech.tis.plugin.ontology.workshop.widget.impl;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.ontology.impl.OntologyPluginMeta;
import com.qlangtech.tis.plugin.ontology.workshop.widget.FullDisplayWidget;
import com.qlangtech.tis.plugin.workshop.widget.IWorkshopWidget;

import java.util.Collections;

/**
 * P0 Widget：饼图（Chart Pie）
 * <p>
 * 饼图 / 环形图，支持标签和数值属性绑定，内置默认色板。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/12
 */
public class ChartPieWidget extends FullDisplayWidget {

    public static final String KEY_DATA_SOURCE_VAR = "dataSourceVar";
    public static final String KEY_LABEL_PROPERTY = "labelProperty";
    public static final String KEY_VALUE_PROPERTY = "valueProperty";

    /** 数据来源：当前模块中的对象集合变量（options 由 Descriptor 动态供给） */
    @FormField(type = FormFieldType.SELECTABLE, ordinal = 10, advance = false)
    public String dataSourceVar;

    @FormField(type = FormFieldType.ENUM, ordinal = 11, advance = false)
    public ChartType chartType = ChartType.pie;

    /**
     * 扇区标签 / 扇区数值取自对象上的哪个属性。
     * <p>
     * 选项随 {@code dataSourceVar} 级联（见 Descriptor 的 {@code valueChangePipe}），
     * 因此两者必须是<b>同层普通字段</b>（见 memory：valueChangePipe 要求 primary 是同层普通字段）。
     */
    @FormField(type = FormFieldType.SELECTABLE, ordinal = 12, advance = false)
    public String labelProperty;

    @FormField(type = FormFieldType.SELECTABLE, ordinal = 13, advance = false)
    public String valueProperty;

    /** 饼图形态 */
    public enum ChartType implements DescriptorUseableShortComment {
        pie("饼图"), doughnut("环形图");
        public final String label;

        ChartType(String label) {
            this.label = label;
        }

        @Override
        public String shortComment() {
            return this.label;
        }
    }

    @TISExtension
    public static class DescriptorImpl extends BaseWidgetDescriptor<IWorkshopWidget> {

        public DescriptorImpl() {
            super();
            this.registerSelectOptions(KEY_DATA_SOURCE_VAR, WidgetOptionHelper::getObjectSetVariableOptions);

            // === 级联属性字段：初始为空（未选 dataSourceVar 时无选项）===
            this.registerSelectOptions(KEY_LABEL_PROPERTY, Collections::emptyList);
            this.registerSelectOptions(KEY_VALUE_PROPERTY, Collections::emptyList);

            this.valueChangePipe(KEY_DATA_SOURCE_VAR, KEY_LABEL_PROPERTY, KEY_VALUE_PROPERTY)
                    .render((pluginMeta, params) -> {
                        String selectedVar = params.getString(KEY_DATA_SOURCE_VAR);
                        if (selectedVar == null) {
                            return Collections.emptyList();
                        }
                        return WidgetOptionHelper.getObjectPropertyOptions(
                                selectedVar, OntologyPluginMeta.createPluginMeta(pluginMeta));
                    });
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