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
import com.qlangtech.tis.plugin.ontology.impl.OntologyPluginMeta;
import com.qlangtech.tis.plugin.workshop.widget.IWorkshopWidget;
import com.qlangtech.tis.plugin.workshop.widget.WorkshopWidget;

import java.util.Collections;

/**
 * P1 Widget：透视表（Pivot Table）
 * <p>
 * 基于 nz-table 动态列实现，绑定对象集变量，
 * 支持按行维度、列维度、度量三维配置，可选择聚合方式。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/9
 */
public class PivotTableWidget extends WorkshopWidget {

    public static final String KEY_OBJECT_SET_VAR = "objectSetVar";
    public static final String KEY_ROW_PROPERTY = "rowProperty";
    public static final String KEY_COLUMN_PROPERTY = "columnProperty";
    public static final String KEY_MEASURE_PROPERTY = "measureProperty";

    /** 对象集输入变量 */
    @FormField(type = FormFieldType.SELECTABLE, ordinal = 13, validate = {Validator.require})
    public String objectSetVar;

    /** 行维度属性 */
    @FormField(type = FormFieldType.SELECTABLE, ordinal = 14, validate = {Validator.require})
    public String rowProperty;

    /** 列维度属性 */
    @FormField(type = FormFieldType.SELECTABLE, ordinal = 15, validate = {Validator.require})
    public String columnProperty;

    /** 度量属性 */
    @FormField(type = FormFieldType.SELECTABLE, ordinal = 16, validate = {Validator.require})
    public String measureProperty;

    /** 聚合方式 */
    @FormField(type = FormFieldType.ENUM, ordinal = 17, validate = {Validator.require})
    public AggregationType aggregation;

    /** 聚合方式枚举 */
    public enum AggregationType {
        sum("求和"), count("计数"), avg("平均值"), min("最小值"), max("最大值");
        public final String label;
        AggregationType(String label) { this.label = label; }
    }

    @TISExtension
    public static class DescriptorImpl extends BaseWidgetDescriptor<IWorkshopWidget> {

        public DescriptorImpl() {
            super();

            // === 对象集变量选项 ===
            this.registerSelectOptions(KEY_OBJECT_SET_VAR, WidgetOptionHelper::getObjectSetVariableOptions);

            // === 级联属性字段：初始为空（未选 objectSetVar 时无选项）===
            this.registerSelectOptions(KEY_ROW_PROPERTY, Collections::emptyList);
            this.registerSelectOptions(KEY_COLUMN_PROPERTY, Collections::emptyList);
            this.registerSelectOptions(KEY_MEASURE_PROPERTY, Collections::emptyList);

            // === 级联管道：objectSetVar 变化时，更新 rowProperty/columnProperty/measureProperty ===
            this.valueChangePipe(KEY_OBJECT_SET_VAR, KEY_ROW_PROPERTY, KEY_COLUMN_PROPERTY, KEY_MEASURE_PROPERTY)
                    .render((pluginMeta, params) -> {
                        String selectedVar = params.getString(KEY_OBJECT_SET_VAR);
                        if (selectedVar == null) {
                            return Collections.emptyList();
                        }
                        return WidgetOptionHelper.getObjectPropertyOptions(
                                selectedVar, OntologyPluginMeta.createPluginMeta(pluginMeta));
                    });
        }

        @Override
        public String getDisplayName() {
            return "Pivot Table";
        }

        @Override
        public WorkShopWidgetType getWidgetType() {
            return WorkShopWidgetType.PIVOT_TABLE;
        }
    }
}