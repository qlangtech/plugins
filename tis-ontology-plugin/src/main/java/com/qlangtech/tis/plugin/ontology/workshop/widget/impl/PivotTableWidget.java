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
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.AggregationFunc;
import com.qlangtech.tis.plugin.ontology.aggregation.AggregationCriteria;
import com.qlangtech.tis.plugin.ontology.aggregation.AggregationResult;
import com.qlangtech.tis.plugin.ontology.aggregation.GroupKey;
import com.qlangtech.tis.plugin.ontology.impl.OntologyPluginMeta;
import com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopVariable;
import com.qlangtech.tis.plugin.ontology.workshop.model.variable.ObjectSetVariable;
import com.qlangtech.tis.plugin.ontology.workshop.widget.WorkshopWidget;
import com.qlangtech.tis.plugin.workshop.widget.IWorkshopWidget;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.util.AttrValMap;
import com.qlangtech.tis.util.IPluginContext;
import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.Map;

/**
 * P1 Widget：透视表（Pivot Table）
 * <p>
 * 基于 nz-table 动态列实现，绑定对象集变量，
 * 支持按行维度、列维度、度量三维配置，可选择聚合方式。
 *
 * <h3>聚合下推（2026-11 迁移）</h3>
 * 改造前本 widget 把对象集的<b>全部明细行</b>拉到浏览器里再用 JS 聚合 —— 是这条链路里最坏的形态
 * （行数越多越卡，且对象集的过滤条件要在前端重算一遍）。现在走 {@code get_data}，一次
 * {@code GROUP BY 行维度, 列维度} 的聚合在数据库里完成，前端只做「聚合行 → 宽表」的形状变换。
 *
 * <p>⚠️ <b>两处语义变化必须知道</b>：
 * <ol>
 *   <li>{@code count} 由「数 measure 的非空值个数」变成 {@code COUNT(*)}。{@link AggregationFunc#COUNT}
 *       不接受 property 参数（见 {@code AggregationCriteria#addMetric}），有 null 度量值时数值会变大
 *       —— 但那才是「该单元格有多少条记录」的正确语义。</li>
 *   <li>{@code AggregationNotSupportedException} <b>不捕获、不回落内存聚合</b>（与
 *       {@code ChartXYDataProcessor#executeAggregation} 同一立场）。静默回落会让大表从「慢」变成
 *       「OOM」，还会掩盖「该方言还没接」这个事实。前端收到 {@code !success} 走
 *       {@code reportDataInvalid}，用户看到明确报错而不是一张空表。</li>
 * </ol>
 *
 * <p><b>分组数上限</b>：{@link AggregationCriteria#DEFAULT_MAX_GROUPS}（50000）。达到上限时
 * {@link AggregationResult#truncated()} 为 true，前端必须提示「结果已截断」，不能静默。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/9
 */
public class PivotTableWidget extends WorkshopWidget {

    public static final String KEY_OBJECT_SET_VAR = "objectSetVar";
    public static final String KEY_ROW_PROPERTY = "rowProperty";
    public static final String KEY_COLUMN_PROPERTY = "columnProperty";
    public static final String KEY_MEASURE_PROPERTY = "measureProperty";
    public static final String KEY_AGGREGATION = "aggregation";

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

    /** 枚举值 → 可下推的聚合函数。{@code count} 恒映射到 {@link AggregationFunc#COUNT}（{@code COUNT(*)}） */
    private static AggregationFunc toAggregationFunc(AggregationType type) {
        return switch (type) {
            case sum -> AggregationFunc.SUM;
            case count -> AggregationFunc.COUNT;
            case avg -> AggregationFunc.AVG;
            case min -> AggregationFunc.MIN;
            case max -> AggregationFunc.MAX;
        };
    }

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

            // === 对象集变量选项（只列可查询的「对象集合定义」型）===
            this.registerSelectOptions(KEY_OBJECT_SET_VAR, WidgetOptionHelper::getObjectSetSourceVariableOptions);

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

        /**
         * 服务端聚合取数。
         *
         * <p>返回 {@code {rows: [{c0, c1, m0}], truncated: boolean}} —— 别名契约见
         * {@link AggregationCriteria#groupAlias(int)} / {@link AggregationCriteria#metricAlias(int)}。
         * 前端只做形状变换（聚合行 → 宽表），不再做任何运算。
         *
         * <p><b>三个属性都必须先进 targetCols</b>：{@code addGroupKey} / {@code addMetric} 的白名单
         * 判据就是 {@code col ∈ targetCols}，漏一个是 {@code IllegalArgumentException}。
         *
         * <p><b>聚合路径不设 orderBy</b>：{@code DataSourceBinding#executeAggregation} 明确拒绝
         * orderByCol。透视表的行/列顺序由前端对少量聚合行排序决定（与 {@code ChartXYDataProcessor}
         * 同一立场）。
         */
        @Override
        public void httpProcess(IControlMsgHandler paramGetter, IPluginContext pluginContext, Context context) throws Exception {
            String methodType = paramGetter.getString("type");
            switch (methodType) {
                case "get_data": {
                    JSONObject postContent = pluginContext.getJSONPostContent();
                    AttrValMap.RelevantFieldsContextAttrValMap<PivotTableWidget> attrs
                            = AttrValMap.createRelevantFieldsContext(paramGetter, context, postContent,
                            (desc) -> Lists.newArrayList(KEY_OBJECT_SET_VAR, KEY_ROW_PROPERTY
                                    , KEY_COLUMN_PROPERTY, KEY_MEASURE_PROPERTY, KEY_AGGREGATION));
                    if (!attrs.validate().isValid()) {
                        return;
                    }
                    PivotTableWidget widget = attrs.createPluginInstance();

                    ObjectSetVariable objSetVar = WorkshopVariable.loadDetail(widget.objectSetVar);

                    AggregationCriteria criteria = new AggregationCriteria();
                    // 三个属性先进白名单，addGroupKey / addMetric 才认它们
                    criteria.addTargetCol(widget.rowProperty);
                    criteria.addTargetCol(widget.columnProperty);
                    criteria.addTargetCol(widget.measureProperty);
                    // row × column 两个分组键分别落到 c0 / c1
                    criteria.addGroupKey(GroupKey.rawColumn(widget.rowProperty));
                    criteria.addGroupKey(GroupKey.rawColumn(widget.columnProperty));
                    AggregationFunc func = toAggregationFunc(
                            widget.aggregation != null ? widget.aggregation : AggregationType.sum);
                    // ⚠️ COUNT 只接受 null property（COUNT(*)，见 AggregationCriteria#addMetric）
                    criteria.addMetric(func, func == AggregationFunc.COUNT ? null : widget.measureProperty);
                    criteria.setMaxGroups(AggregationCriteria.DEFAULT_MAX_GROUPS);

                    // 中继条件必须早于聚合；静态条件由 executeAggregation 内部施加
                    ObjectTableWidget.DescriptorImpl.applyRuntimeFilters(paramGetter, objSetVar, criteria);
                    AggregationResult result = objSetVar.executeAggregation(criteria);

                    // 截断标志必须透出去：分组数达上限时静默返回前 50000 组，用户会以为数据就这么点
                    paramGetter.setBizResult(context, Map.of(
                            "rows", result.rows(), "truncated", result.truncated()));
                    return;
                }
                default:
                    throw new IllegalStateException("illegal methodType:" + methodType);
            }
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