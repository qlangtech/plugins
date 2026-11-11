package com.qlangtech.tis.plugin.ontology.workshop.widget.impl;

import com.alibaba.citrus.turbine.Context;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.ibatis.RowMap;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.ontology.impl.objtype.ObjectSetRowQuery;
import com.qlangtech.tis.plugin.ontology.impl.objtype.ObjectTypeBinding;
import com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopVariable;
import com.qlangtech.tis.plugin.ontology.workshop.model.variable.ObjectSetVariable;
import com.qlangtech.tis.plugin.ontology.workshop.widget.WorkshopWidget;
import com.qlangtech.tis.plugin.workshop.widget.IWorkshopWidget;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.util.AttrValMap;
import com.qlangtech.tis.util.IPluginContext;

import java.util.List;

/**
 * P0 Widget：对象下拉选择（Object Dropdown）
 * <p>
 * 下拉选择器，从对象集合中选择一个对象，支持搜索过滤和输出变量绑定。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/12
 */
public class ObjectDropdownWidget extends WorkshopWidget {

    public static final String KEY_OBJECT_SET_VAR = "objectSetVar";
    public static final String KEY_SELECTED_OBJECT_VAR = "selectedObjectVar";

    /** 候选对象来源：当前模块中的对象集合变量（options 由 Descriptor 动态供给） */
    @FormField(type = FormFieldType.SELECTABLE,ordinal = 10)
    public String objectSetVar;

    /** 选中项写回：下拉选中一个对象后写入该变量，供下游组件消费 */
    @FormField(type = FormFieldType.SELECTABLE,ordinal = 11)
    public String selectedObjectVar;

    @TISExtension
    public static class DescriptorImpl extends BaseWidgetDescriptor<IWorkshopWidget> {

        public DescriptorImpl() {
            super();
            // 数据源只列「对象集合定义」的变量（取数需要可查询的 schema）；
            // 写回载体（selectedObjectVar）反过来必须是任意 OBJECT_SET —— 外部写入型正是它的典型用法。
            this.registerSelectOptions(KEY_OBJECT_SET_VAR, WidgetOptionHelper::getObjectSetSourceVariableOptions);
            this.registerSelectOptions(KEY_SELECTED_OBJECT_VAR, WidgetOptionHelper::getObjectSetVariableOptions);
        }

        /**
         * 服务端取数：下拉的候选对象。
         *
         * <p>取<b>全部</b>属性而不只是主键：模板里选项文案写死读 {@code item.name}，
         * 而后端并不知道「name」是不是用户配置——若每个对象类型都只取一列，别的类型在
         * 下拉里会全部显示成空白。行数封顶 {@link ObjectSetRowQuery#DEFAULT_MAX_ROWS}。
         */
        @Override
        public void httpProcess(IControlMsgHandler paramGetter, IPluginContext pluginContext, Context context) throws Exception {
            String methodType = paramGetter.getString("type");
            switch (methodType) {
                case "get_data": {
                    JSONObject postContent = pluginContext.getJSONPostContent();
                    AttrValMap.RelevantFieldsContextAttrValMap<ObjectDropdownWidget> attrs
                            = AttrValMap.createRelevantFieldsContext(paramGetter, context, postContent,
                            (desc) -> Lists.newArrayList(KEY_OBJECT_SET_VAR));
                    if (!attrs.validate().isValid()) {
                        return;
                    }
                    ObjectDropdownWidget widget = attrs.createPluginInstance();

                    ObjectSetVariable objSetVar = WorkshopVariable.loadDetail(widget.objectSetVar);
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
            return "Object Dropdown";
        }

        @Override
        public WorkShopWidgetType getWidgetType() {
            return WorkShopWidgetType.OBJECT_DROPDOWN;
        }
    }
}