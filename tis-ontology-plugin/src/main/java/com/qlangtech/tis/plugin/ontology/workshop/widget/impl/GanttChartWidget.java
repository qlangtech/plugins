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

import com.alibaba.citrus.turbine.Context;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.ibatis.RowMap;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.impl.OntologyPluginMeta;
import com.qlangtech.tis.plugin.ontology.impl.objtype.ObjectSetRowQuery;
import com.qlangtech.tis.plugin.ontology.impl.objtype.ObjectTypeBinding;
import com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopVariable;
import com.qlangtech.tis.plugin.ontology.workshop.model.variable.ObjectSetVariable;
import com.qlangtech.tis.plugin.ontology.workshop.widget.WorkshopWidget;
import com.qlangtech.tis.plugin.workshop.widget.IWorkshopWidget;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.util.AttrValMap;
import com.qlangtech.tis.util.IPluginContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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

            // === 对象集变量选项（只列可查询的「对象集合定义」型，见 getObjectSetSourceVariableOptions）===
            this.registerSelectOptions(KEY_OBJECT_SET_VAR, WidgetOptionHelper::getObjectSetSourceVariableOptions);

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

        /**
         * 服务端取数：标签 / 开始 / 结束三列 + 主键，行数封顶 {@link ObjectSetRowQuery#DEFAULT_MAX_ROWS}。
         *
         * <p>与前端 {@code gantt-chart.component.ts} 的 {@code fetchRows()} 一一对应。
         */
        @Override
        public void httpProcess(IControlMsgHandler paramGetter, IPluginContext pluginContext, Context context) throws Exception {
            String methodType = paramGetter.getString("type");
            switch (methodType) {
                case "get_data": {
                    JSONObject postContent = pluginContext.getJSONPostContent();
                    AttrValMap.RelevantFieldsContextAttrValMap<GanttChartWidget> attrs
                            = AttrValMap.createRelevantFieldsContext(paramGetter, context, postContent,
                            (desc) -> Lists.newArrayList(KEY_OBJECT_SET_VAR, KEY_LABEL_PROP, KEY_START_PROP, KEY_END_PROP));
                    if (!attrs.validate().isValid()) {
                        return;
                    }
                    GanttChartWidget widget = attrs.createPluginInstance();

                    ObjectSetVariable objSetVar = WorkshopVariable.loadDetail(widget.objectSetVar);
                    List<String> targetCols = new ArrayList<>(3);
                    targetCols.add(widget.labelProp);
                    targetCols.add(widget.startProp);
                    targetCols.add(widget.endProp);

                    ObjectTypeBinding.DataSourceQueryCriteria criteria
                            = ObjectSetRowQuery.createCriteria(targetCols, objSetVar.getIdentityPropName()
                            , ObjectSetRowQuery.DEFAULT_MAX_ROWS);
                    ObjectTableWidget.DescriptorImpl.applyRuntimeFilters(paramGetter, objSetVar, criteria);
                    List<RowMap> rows = objSetVar.executeQuery(criteria);
                    paramGetter.setBizResult(context, rows);
                    return;
                }
                default:
                    throw new IllegalStateException("illegal methodType:" + methodType);
            }
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