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
import com.qlangtech.tis.plugin.ontology.workshop.widget.WorkshopWidget;
import com.qlangtech.tis.plugin.workshop.widget.IWorkshopWidget;

import java.util.Collections;

/**
 * P1 Widget：甘特图（Gantt Chart）
 * <p>
 * 基于 chart.js + ng2-charts 的横向条形图模拟甘特图。
 * 绑定对象集变量，每个对象包含标签、开始时间和结束时间属性。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/9
 */
public class GanttChartWidget extends WorkshopWidget {

    public static final String KEY_OBJECT_SET_VAR = "objectSetVar";
    public static final String KEY_LABEL_PROP = "labelProp";
    public static final String KEY_START_PROP = "startProp";
    public static final String KEY_END_PROP = "endProp";

    /** 对象集输入变量 */
    @FormField(type = FormFieldType.SELECTABLE, ordinal = 13, validate = {Validator.require})
    public String objectSetVar;

    /** 标签属性（对象中作为任务名称的属性） */
    @FormField(type = FormFieldType.SELECTABLE, ordinal = 14, validate = {Validator.require})
    public String labelProp;

    /** 开始时间属性 */
    @FormField(type = FormFieldType.SELECTABLE, ordinal = 15, validate = {Validator.require})
    public String startProp;

    /** 结束时间属性 */
    @FormField(type = FormFieldType.SELECTABLE, ordinal = 16, validate = {Validator.require})
    public String endProp;

    @TISExtension
    public static class DescriptorImpl extends BaseWidgetDescriptor<IWorkshopWidget> {

        public DescriptorImpl() {
            super();

            // === 对象集变量选项 ===
            this.registerSelectOptions(KEY_OBJECT_SET_VAR, WidgetOptionHelper::getObjectSetVariableOptions);

            // === 级联属性字段：初始为空（未选 objectSetVar 时无选项）===
            this.registerSelectOptions(KEY_LABEL_PROP, Collections::emptyList);
            this.registerSelectOptions(KEY_START_PROP, Collections::emptyList);
            this.registerSelectOptions(KEY_END_PROP, Collections::emptyList);

            // === 级联管道：objectSetVar 变化时，更新 labelProp/startProp/endProp ===
            this.valueChangePipe(KEY_OBJECT_SET_VAR, KEY_LABEL_PROP, KEY_START_PROP, KEY_END_PROP)
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
            return "Gantt Chart";
        }

        @Override
        public WorkShopWidgetType getWidgetType() {
            return WorkShopWidgetType.GANTT_CHART;
        }
    }
}