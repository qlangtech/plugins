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
 * {@link InterfaceInputRow} 的元组行编辑器工厂。
 *
 * <p>由 {@code MappingInterfaceConfig.json} 里 {@code inputs} 字段的 {@code elementCreator}
 * 按 FQCN 反射实例化（见 {@code MultiItemsViewType.createMultiItemsViewType}，走
 * {@code newInstance()}），因此<b>必须有无参构造器</b>。
 *
 * <h3>线格式约定</h3>
 * {@link #getTuplesKey()} 返回 {@code "_interfaceInputs"}，前端
 * {@code InterfaceInputRowsProperty}（{@code src/base/common/ontology.common.ts}）
 * 必须把行数组挂在<b>同名</b>的字段上：整个 TuplesProperty 对象会被整体 JSON 化后放在
 * {@code _eprops.enum} 下提交上来，服务端 {@code ViewFormatType.TupleList} 正是按
 * {@code enum.<getTuplesKey()>} 取出行数组回传给 {@link #parsePostMCols}。
 * key 写错不报错，只会静默解析出 0 行。
 *
 * <h3>为什么 {@code createDefault} 不能返回 null</h3>
 * {@code MultiItemsViewType.getElementPropertyKeys()} 会对
 * {@code createDefault(new JSONObject())} 的结果取 JavaBean 属性描述符来生成
 * {@code elementKeys}，返回 null 会直接抛异常。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/16
 * @see InterfaceInputRow
 * @see RoutingParamCreatorFactory
 */
public class InterfaceInputCreatorFactory implements ElementCreatorFactory<InterfaceInputRow> {

    /**
     * 与前端 {@code InterfaceInputRowsProperty._interfaceInputs} 字段同名。
     */
    public static final String TUPLES_KEY = "_interfaceInputs";

    public static final String KEY_PARAMETER = "parameter";
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
        return ViewContent.InterfaceInputRows;
    }

    /**
     * 把变量候选列表随 {@code _eprops.enum} 下发。
     *
     * <p>不把候选集放在行模型字段上：元组行的字段拿不到所在行的上下文，
     * 而这一列的可选值<b>逐行相同</b>，由工厂统一下发最省事，也与
     * {@code FilterItemElementCreatorFactory} 下发 {@code components}/{@code operators}
     * 的做法一致。
     */
    @Override
    public void appendExternalJsonProp(IPropertyType propertyType, JSONObject biz) {
        biz.put(KEY_VARIABLE_OPTS, Option.toJson(WidgetOptionHelper.getWorkshopVariableOptions()));
    }

    @Override
    public CMeta.ParsePostMCols<InterfaceInputRow> parsePostMCols(//
            IPropertyType propertyType, IControlMsgHandler msgHandler, Context context,
            String keyColsMeta, JSONArray targetCols) {
        CMeta.ParsePostMCols<InterfaceInputRow> parseResult = new CMeta.ParsePostMCols<>();
        if (targetCols == null) {
            // 前端没有提交映射表：视为空列表，不算错误（inputs 本身不是必填）
            return parseResult;
        }

        Map<String, Collection<Integer>> duplicateParams = Maps.newHashMap();
        for (int i = 0; i < targetCols.size(); i++) {
            final int tupleIndex = i;
            JSONObject tuple = targetCols.getJSONObject(i);

            // 逐行包一层 msgHandler，把错误挂到 "inputs[3].parameter" 这样的字段路径上
            IControlMsgHandler rowMsgHandler = new BasicDelegateMsgHandler(msgHandler) {
                @Override
                public void addFieldError(Context context, String fieldName, String msg, Object... params) {
                    super.addFieldError(context, IFieldErrorHandler.joinField(keyColsMeta //
                            , Collections.singletonList(tupleIndex), fieldName), msg, params);
                }
            };

            InterfaceInputRow item = this.createDefault(tuple);
            item.parameter = tuple.getString(KEY_PARAMETER);
            item.variableName = tuple.getString(KEY_VARIABLE_NAME);

            if (StringUtils.isEmpty(item.parameter)) {
                rowMsgHandler.addFieldError(context, KEY_PARAMETER, ValidatorCommons.MSG_EMPTY_INPUT_ERROR);
            } else {
                Collection<Integer> sameParams = duplicateParams.get(item.parameter);
                if (sameParams == null) {
                    sameParams = Lists.newArrayList();
                    duplicateParams.put(item.parameter, sameParams);
                }
                sameParams.add(i);
            }
            if (StringUtils.isEmpty(item.variableName)) {
                rowMsgHandler.addFieldError(context, KEY_VARIABLE_NAME, ValidatorCommons.MSG_EMPTY_INPUT_ERROR);
            }

            parseResult.writerCols.add(item);
        }

        // 同一个参数名出现两次：后一行会静默覆盖前一行，构建者看不出哪一行生效
        for (Map.Entry<String, Collection<Integer>> entry : duplicateParams.entrySet()) {
            if (entry.getValue().size() > 1) {
                for (Integer duplicateIndex : entry.getValue()) {
                    msgHandler.addFieldError(context, IFieldErrorHandler.joinField(keyColsMeta //
                            , Collections.singletonList(duplicateIndex), KEY_PARAMETER)
                            , IdentityName.MSG_ERROR_NAME_DUPLICATE);
                }
            }
        }

        parseResult.validateFaild = context.hasErrors();
        return parseResult;
    }

    @Override
    public InterfaceInputRow createDefault(JSONObject targetCol) {
        return new InterfaceInputRow();
    }

    @Override
    public InterfaceInputRow create(JSONObject targetCol, BiConsumer<String, String> errorProcess) {
        InterfaceInputRow item = this.createDefault(targetCol);
        item.parameter = targetCol.getString(KEY_PARAMETER);
        item.variableName = targetCol.getString(KEY_VARIABLE_NAME);
        return item;
    }
}
