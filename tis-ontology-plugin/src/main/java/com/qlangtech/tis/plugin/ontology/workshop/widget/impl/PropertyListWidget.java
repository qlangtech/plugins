/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
 * P1 Widget：属性列表（Property List）
 * <p>
 * 分组标题直接用基类的 {@code title} 字段（前端读的也是 {@code config.title}），
 * 这里原先又声明了一个同名 {@code title} 把基类字段遮蔽掉 —— 表单反射整条继承链的字段，
 * 同名会让字段集合出现两个 {@code title}，配置值到底落在哪一个不确定。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/9
 */
public class PropertyListWidget extends FullDisplayWidget implements IWidgetColumnHost {

    public static final String KEY_OBJECT_VAR = "objectVar";
    public static final String KEY_PROPERTIES = "properties";

    /** 展示对象来源：当前模块中的对象集合变量（options 由 Descriptor 动态供给） */
    @FormField(type = FormFieldType.SELECTABLE, ordinal = 10, advance = false)
    public String objectVar;

    @FormField(type = FormFieldType.INT_NUMBER, ordinal = 12, advance = false, validate = {Validator.require, Validator.integer})
    public Integer columnsCount;

    /**
     * 要展示的属性列表，由子表单结构化管理（非手写 JSON）。
     * <p>
     * 原先声明为 {@code MULTI_SELECTABLE} 且没有登记行编辑器，前端拿到的是一个填不进去的
     * 多选下拉；改为 {@code MULTI_DESCRIBLE_PLUGIN} 后由 {@link WidgetColumnConfig} 承担行结构，
     * 前端读的是同一批 {@code prop}/{@code label}/{@code format} 字段。
     */
    @FormField(type = FormFieldType.MULTI_DESCRIBLE_PLUGIN, desClazz = WidgetColumnConfig.class,
            ordinal = 11, advance = false, validate = {Validator.require})
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
            return "Property List";
        }

        @Override
        public WorkShopWidgetType getWidgetType() {
            return WorkShopWidgetType.PROPERTY_LIST;
        }
    }
}