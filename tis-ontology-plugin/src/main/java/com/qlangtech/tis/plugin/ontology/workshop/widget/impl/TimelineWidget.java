package com.qlangtech.tis.plugin.ontology.workshop.widget.impl;

import com.alibaba.citrus.turbine.Context;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.ibatis.RowMap;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.ontology.impl.objtype.ObjectSetRowQuery;
import com.qlangtech.tis.plugin.ontology.impl.objtype.ObjectTypeBinding;
import com.qlangtech.tis.plugin.ontology.workshop.enums.VariableType;
import com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopVariable;
import com.qlangtech.tis.plugin.ontology.workshop.model.variable.ObjectSetVariable;
import com.qlangtech.tis.plugin.ontology.workshop.widget.WorkshopWidget;
import com.qlangtech.tis.plugin.workshop.widget.IWorkshopWidget;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.util.AttrValMap;
import com.qlangtech.tis.util.IPluginContext;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * P0 Widget：时间线（Timeline）
 * <p>
 * 以时间线形式展示事件序列，支持自定义颜色、图标、时间戳和内容。
 *
 * <h3>两种数据源，两条取数路径</h3>
 * <ul>
 *   <li>{@link VariableType#OBJECT_SET} —— 走本 widget 自己的 {@code get_data}（服务端取行）；</li>
 *   <li>{@link VariableType#TIME_SERIES_SET} —— 保留前端值通道。时间序列集合由前端
 *       {@code ObjectPropertyConfig} 计算，后端没有对应的可查询数据源，故 {@code get_data}
 *       对它<b>直接抛</b>（前端不会发这条路，这是入口守卫）。</li>
 * </ul>
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/12
 */
public class TimelineWidget extends WorkshopWidget {

    public static final String KEY_DATA_SOURCE_VAR = "dataSourceVar";
    public static final String KEY_MODE = "mode";

    /** 事件来源：当前模块中的对象集合或时间序列集合变量（options 由 Descriptor 动态供给） */
    @FormField(type = FormFieldType.SELECTABLE,ordinal = 10)
    public String dataSourceVar;

    /** 时间线节点相对轴线的位置 */
    @FormField(type = FormFieldType.ENUM,ordinal = 11)
    public TimelineMode mode = TimelineMode.left;

    /**
     * 条目内部的颜色 / 图标 / 时间戳 / 正文都取自数据本身（前端读 {@code item.color} 等），
     * 不对应配置字段。
     */
    public enum TimelineMode implements DescriptorUseableShortComment {
        left("左侧"), right("右侧"), alternate("交替");
        public final String label;

        TimelineMode(String label) {
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
            // 同时接受两种类型：时间线的数据既可能是带时间属性的对象集合，也可能是时间序列集合。
            // 只认后者会让下拉在只建了对象集合的模块里恒为空。
            this.registerSelectOptions(KEY_DATA_SOURCE_VAR,
                    () -> WidgetOptionHelper.getVariableOptions(
                            VariableType.OBJECT_SET, VariableType.TIME_SERIES_SET));
        }

        /**
         * 服务端取数：仅对 {@link VariableType#OBJECT_SET} 生效。
         *
         * <p>前端按变量类型分派 —— {@code OBJECT_SET} 调这里，{@code TIME_SERIES_SET} 仍走
         * {@code watchValueSafe}（那份数据后端算不出来）。故本分支拿到 TIME_SERIES_SET
         * 说明前端接线错了，直接抛而不是给一张空轴线。
         *
         * <p>取<b>全部</b>属性：模板写死读 {@code item.timestamp / content / color / icon}，
         * 而那些键不存在于对象类型的属性定义里 —— 只能整行给过去，由数据自己决定有没有。
         */
        @Override
        public void httpProcess(IControlMsgHandler paramGetter, IPluginContext pluginContext, Context context) throws Exception {
            String methodType = paramGetter.getString("type");
            switch (methodType) {
                case "get_data": {
                    JSONObject postContent = pluginContext.getJSONPostContent();
                    AttrValMap.RelevantFieldsContextAttrValMap<TimelineWidget> attrs
                            = AttrValMap.createRelevantFieldsContext(paramGetter, context, postContent,
                            (desc) -> Lists.newArrayList(KEY_DATA_SOURCE_VAR, KEY_MODE));
                    if (!attrs.validate().isValid()) {
                        return;
                    }
                    TimelineWidget widget = attrs.createPluginInstance();
                    if (StringUtils.isBlank(widget.dataSourceVar)) {
                        throw new IllegalStateException("Timeline widget dataSourceVar can not be blank");
                    }

                    WorkshopVariable variable = WorkshopVariable.loadDetail(widget.dataSourceVar);
                    if (variable.getVariableType() == VariableType.TIME_SERIES_SET) {
                        throw new IllegalStateException("Timeline 的 TIME_SERIES_SET 数据源由前端计算，"
                                + "不应调用 get_data:" + widget.dataSourceVar);
                    }
                    if (!(variable instanceof ObjectSetVariable objSetVar)) {
                        throw new IllegalStateException("变量 '" + widget.dataSourceVar + "' 不是对象集合变量，"
                                + "实际类型:" + variable.getClass().getSimpleName());
                    }

                    List<String> targetCols = ObjectSetRowQuery.allColNames(objSetVar.getColsMeta());
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
            return "Timeline";
        }

        @Override
        public WorkShopWidgetType getWidgetType() {
            return WorkShopWidgetType.TIMELINE;
        }
    }
}
