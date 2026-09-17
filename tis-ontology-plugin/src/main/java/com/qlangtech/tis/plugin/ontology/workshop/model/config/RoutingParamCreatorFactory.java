package com.qlangtech.tis.plugin.ontology.workshop.model.config;

import com.alibaba.citrus.turbine.Context;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.qlangtech.tis.extension.IPropertyType;
import com.qlangtech.tis.manage.common.Option;
import com.qlangtech.tis.plugin.IdentityName;
import com.qlangtech.tis.plugin.ValidatorCommons;
import com.qlangtech.tis.plugin.ds.CMeta;
import com.qlangtech.tis.plugin.ds.ElementCreatorFactory;
import com.qlangtech.tis.plugin.ds.ViewContent;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.WidgetOptionHelper;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import com.qlangtech.tis.runtime.module.misc.impl.BasicDelegateMsgHandler;
import org.apache.commons.lang.StringUtils;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * {@link RoutingParamRow} 的元组行编辑器工厂。
 *
 * <p>与 {@link InterfaceInputCreatorFactory} 同构，此处不复用其实现：两者的
 * {@code ViewContent} token、{@code getTuplesKey()} 与身份字段名都不同，
 * 而这三个值正是线格式的全部内容，抽公共基类只会把差异藏进抽象方法里。
 * 仓库内其他元组行工厂（{@code OntologyPropertyCreatorFactory} /
 * {@code FilterItemElementCreatorFactory}）同样是各自独立的。
 *
 * <p>由 {@code PageRoutingConfig.json} 里 {@code params} 字段的 {@code elementCreator}
 * 按 FQCN 反射实例化，因此<b>必须有无参构造器</b>。线格式约定见
 * {@link InterfaceInputCreatorFactory} 的类注释（{@code getTuplesKey()} 的键名必须与前端
 * {@code RoutingParamRowsProperty._routingParams} 逐字一致，写错只会静默解析出 0 行）。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/16
 * @see RoutingParamRow
 */
public class RoutingParamCreatorFactory implements ElementCreatorFactory<RoutingParamRow> {

    /**
     * 与前端 {@code RoutingParamRowsProperty._routingParams} 字段同名。
     */
    public static final String TUPLES_KEY = "_routingParams";

    public static final String KEY_URL_PARAM = "urlParam";
    public static final String KEY_VARIABLE_NAME = "variableName";

    /**
     * 变量候选列表的 key，供前端行编辑器渲染 {@code variableName} 列的下拉框。
     */
    public static final String KEY_VARIABLE_OPTS = "variableOpts";

    @Override
    public final String getTuplesKey() {
        return TUPLES_KEY;
    }

    @Override
    public ViewContent getViewContentType() {
        return ViewContent.RoutingParamRows;
    }

    @Override
    public void appendExternalJsonProp(IPropertyType propertyType, JSONObject biz) {
        biz.put(KEY_VARIABLE_OPTS, Option.toJson(WidgetOptionHelper.getWorkshopVariableOptions()));
    }

    @Override
    public CMeta.ParsePostMCols<RoutingParamRow> parsePostMCols(//
            IPropertyType propertyType, IControlMsgHandler msgHandler, Context context,
            String keyColsMeta, JSONArray targetCols) {
        CMeta.ParsePostMCols<RoutingParamRow> parseResult = new CMeta.ParsePostMCols<>();
        if (targetCols == null) {
            return parseResult;
        }

        Map<String, Collection<Integer>> duplicateParams = Maps.newHashMap();
        for (int i = 0; i < targetCols.size(); i++) {
            final int tupleIndex = i;
            JSONObject tuple = targetCols.getJSONObject(i);

            // 逐行包一层 msgHandler，把错误挂到 "params[3].urlParam" 这样的字段路径上
            IControlMsgHandler rowMsgHandler = new BasicDelegateMsgHandler(msgHandler) {
                @Override
                public void addFieldError(Context context, String fieldName, String msg, Object... params) {
                    super.addFieldError(context, IFieldErrorHandler.joinField(keyColsMeta //
                            , Collections.singletonList(tupleIndex), fieldName), msg, params);
                }
            };

            RoutingParamRow item = this.createDefault(tuple);
            item.urlParam = tuple.getString(KEY_URL_PARAM);
            item.variableName = tuple.getString(KEY_VARIABLE_NAME);

            if (StringUtils.isEmpty(item.urlParam)) {
                rowMsgHandler.addFieldError(context, KEY_URL_PARAM, ValidatorCommons.MSG_EMPTY_INPUT_ERROR);
            } else {
                Collection<Integer> sameParams = duplicateParams.get(item.urlParam);
                if (sameParams == null) {
                    sameParams = Lists.newArrayList();
                    duplicateParams.put(item.urlParam, sameParams);
                }
                sameParams.add(i);
            }
            if (StringUtils.isEmpty(item.variableName)) {
                rowMsgHandler.addFieldError(context, KEY_VARIABLE_NAME, ValidatorCommons.MSG_EMPTY_INPUT_ERROR);
            }

            parseResult.writerCols.add(item);
        }

        // 同一个 URL 参数名出现两次：后一行会静默覆盖前一行
        for (Map.Entry<String, Collection<Integer>> entry : duplicateParams.entrySet()) {
            if (entry.getValue().size() > 1) {
                for (Integer duplicateIndex : entry.getValue()) {
                    msgHandler.addFieldError(context, IFieldErrorHandler.joinField(keyColsMeta //
                            , Collections.singletonList(duplicateIndex), KEY_URL_PARAM)
                            , IdentityName.MSG_ERROR_NAME_DUPLICATE);
                }
            }
        }

        parseResult.validateFaild = context.hasErrors();
        return parseResult;
    }

    @Override
    public RoutingParamRow createDefault(JSONObject targetCol) {
        return new RoutingParamRow();
    }

    @Override
    public RoutingParamRow create(JSONObject targetCol, BiConsumer<String, String> errorProcess) {
        RoutingParamRow item = this.createDefault(targetCol);
        item.urlParam = targetCol.getString(KEY_URL_PARAM);
        item.variableName = targetCol.getString(KEY_VARIABLE_NAME);
        return item;
    }
}
