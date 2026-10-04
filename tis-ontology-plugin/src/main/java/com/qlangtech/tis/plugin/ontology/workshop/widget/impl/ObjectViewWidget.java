package com.qlangtech.tis.plugin.ontology.workshop.widget.impl;

import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.workshop.widget.WidgetColumnConfig;
import com.qlangtech.tis.plugin.ontology.workshop.widget.FullDisplayWidget;
import com.qlangtech.tis.plugin.ontology.workshop.widget.IWidgetColumnHost;
import com.qlangtech.tis.plugin.workshop.widget.IWorkshopWidget;

import java.util.List;

/**
 * P0 Widget：对象详情视图（Object View）
 * <p>
 * 以描述列表（nz-descriptions）形式展示单个对象的属性详情。分组标题用基类的 {@code title}。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/12
 */
public class ObjectViewWidget extends FullDisplayWidget implements IWidgetColumnHost {

    public static final String KEY_OBJECT_VAR = "objectVar";
    public static final String KEY_PROPERTIES = "properties";

    /** 展示对象来源：当前模块中的对象集合变量（options 由 Descriptor 动态供给） */
    @FormField(type = FormFieldType.SELECTABLE, ordinal = 10, advance = false)
    public String objectVar;

    /** nz-descriptions 的列数 */
    @FormField(type = FormFieldType.INT_NUMBER, ordinal = 11, advance = false, validate = {Validator.integer})
    public Integer columnCount;

    /**
     * 要展示的属性列表，由子表单结构化管理。
     * <p>
     * 与 {@link PropertyListWidget#properties} 是同一个模型；前端读的字段名统一按
     * {@link WidgetColumnConfig#prop} 来，ObjectView 组件此前读的是 {@code prop.key}，
     * 取不到值只能渲染空白行。
     */
    @FormField(type = FormFieldType.MULTI_DESCRIBLE_PLUGIN, desClazz = WidgetColumnConfig.class,
            ordinal = 12, advance = false)
    public List<WidgetColumnConfig> properties;

    @Override
    public String getBoundObjectSetVar() {
        return this.objectVar;
    }

    @Override
    public List<WidgetColumnConfig> getColumnConfigs() {
        return this.properties;
    }

    @TISExtension
    public static class DescriptorImpl extends BaseWidgetDescriptor<IWorkshopWidget> {

        public DescriptorImpl() {
            super();
            this.registerSelectOptions(KEY_OBJECT_VAR, WidgetOptionHelper::getObjectSetVariableOptions);

            // === 级联管道：objectVar 变化时，按该对象集变量的属性自动生成候选属性行 ===
            this.valueChangePipe(KEY_OBJECT_VAR, KEY_PROPERTIES).render((pluginMeta, param) ->
                    WidgetColumnConfig.createFrom(
                            WidgetOptionHelper.getObjectPropertyMetas(param.getString(KEY_OBJECT_VAR))));
        }

        @Override
        public String getDisplayName() {
            return "Object View";
        }

        @Override
        public WorkShopWidgetType getWidgetType() {
            return WorkShopWidgetType.OBJECT_VIEW;
        }
    }
}