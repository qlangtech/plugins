package com.qlangtech.tis.plugin.ontology.workshop.widget.impl;

import com.alibaba.citrus.turbine.Context;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.ibatis.RowMap;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
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
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * P0 Widget：饼图（Chart Pie）
 * <p>
 * 饼图 / 环形图，支持标签和数值属性绑定，内置默认色板。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/12
 */
public class ChartPieWidget extends WorkshopWidget {

    public static final String KEY_DATA_SOURCE_VAR = "dataSourceVar";
    public static final String KEY_LABEL_PROPERTY = "labelProperty";
    public static final String KEY_VALUE_PROPERTY = "valueProperty";

    /** 数据来源：当前模块中的对象集合变量（options 由 Descriptor 动态供给） */
    @FormField(type = FormFieldType.SELECTABLE,ordinal = 10)
    public String dataSourceVar;

    @FormField(type = FormFieldType.ENUM,ordinal = 11)
    public ChartType chartType = ChartType.pie;

    /**
     * 扇区标签 / 扇区数值取自对象上的哪个属性。
     * <p>
     * 选项随 {@code dataSourceVar} 级联（见 Descriptor 的 {@code valueChangePipe}），
     * 因此两者必须是<b>同层普通字段</b>（见 memory：valueChangePipe 要求 primary 是同层普通字段）。
     */
    @FormField(type = FormFieldType.SELECTABLE,ordinal = 12)
    public String labelProperty;

    @FormField(type = FormFieldType.SELECTABLE,ordinal = 13)
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
            // 数据源只列「对象集合定义」的变量：本 widget 的 get_data 按对象集变量名取行，
            // 「函数计算 / 外部写入」定义的对象集没有可查询的 schema，取数时会抛 illegal type。
            this.registerSelectOptions(KEY_DATA_SOURCE_VAR, WidgetOptionHelper::getObjectSetSourceVariableOptions);

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

        /**
         * 服务端取数。
         *
         * <p>与前端 {@code chart-pie.component.ts} 的 {@code fetchRows()} 对应：请求体是整棵 widget
         * 的表单投影，query string 只带 {@code filters}（中继条件）。
         *
         * <p>只取「扇区标签 + 扇区数值」两列（加主键）：饼图不需要明细行里的其它属性，
         * 少取一列就少一次序列化。行数封顶 {@link ObjectSetRowQuery#DEFAULT_MAX_ROWS}。
         */
        @Override
        public void httpProcess(IControlMsgHandler paramGetter, IPluginContext pluginContext, Context context) throws Exception {
            String methodType = paramGetter.getString("type");
            switch (methodType) {
                case "get_data": {
                    JSONObject postContent = pluginContext.getJSONPostContent();
                    // ⚠️ 白名单漏列某个字段 = 它恒为 null 且不报错（见 AttrValMap 的契约）
                    AttrValMap.RelevantFieldsContextAttrValMap<ChartPieWidget> attrs
                            = AttrValMap.createRelevantFieldsContext(paramGetter, context, postContent,
                            (desc) -> Lists.newArrayList(KEY_DATA_SOURCE_VAR, KEY_LABEL_PROPERTY, KEY_VALUE_PROPERTY));
                    if (!attrs.validate().isValid()) {
                        return;
                    }
                    ChartPieWidget widget = attrs.createPluginInstance();

                    ObjectSetVariable objSetVar = WorkshopVariable.loadDetail(widget.dataSourceVar);
                    List<String> targetCols = new ArrayList<>(2);
                    targetCols.add(widget.labelProperty);
                    targetCols.add(widget.valueProperty);

                    ObjectTypeBinding.DataSourceQueryCriteria criteria
                            = ObjectSetRowQuery.createCriteria(targetCols, objSetVar.getIdentityPropName()
                            , ObjectSetRowQuery.DEFAULT_MAX_ROWS);
                    // 中继条件只有前端知道；静态条件由 ObjectSetVariable 内部施加
                    ObjectTableWidget.DescriptorImpl.applyRuntimeFilters(paramGetter, objSetVar, criteria);
                    // ⚠️ 不能走 fetchRows：那条路自己造 criteria，会把上面刚施加的中继条件丢掉
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
            return "Chart Pie";
        }

        @Override
        public WorkShopWidgetType getWidgetType() {
            return WorkShopWidgetType.CHART_PIE;
        }
    }
}