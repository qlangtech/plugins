package com.qlangtech.tis.plugin.ontology.workshop.widget;

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
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import com.qlangtech.tis.runtime.module.misc.impl.BasicDelegateMsgHandler;
import org.apache.commons.lang.StringUtils;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * {@link FilterItemConfig} 的元组行编辑器工厂。
 *
 * <p>由 {@code FilterListWidget.json} 里 {@code filters} 字段的 {@code elementCreator} 按 FQCN
 * 反射实例化（见 {@code MultiItemsViewType.createMultiItemsViewType}，走 {@code newInstance()}），
 * 因此<b>必须有无参构造器</b>。
 *
 * <h3>线格式约定</h3>
 * {@link #getTuplesKey()} 返回 {@code "_filters"}，前端 {@code WorkshopFilterItemsProperty}
 * （{@code src/base/common/ontology.common.ts}）必须把行数组挂在<b>同名</b>的字段上：
 * 整个 TuplesProperty 对象会被整体 JSON 化后放在 {@code _eprops.enum} 下提交上来，
 * 服务端 {@code ViewFormatType.TupleList} 正是按 {@code enum.<getTuplesKey()>} 取出行数组
 * 回传给 {@link #parsePostMCols}。key 写错不报错，只会静默解析出 0 行。
 *
 * <h3>为什么 {@code createDefault} 不能返回 null</h3>
 * {@code MultiItemsViewType.getElementPropertyKeys()} 会对 {@code createDefault(new JSONObject())}
 * 的结果取 JavaBean 属性描述符来生成 {@code elementKeys}，返回 null 会直接抛异常。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/16
 * @see FilterItemConfig
 */
public class FilterItemElementCreatorFactory implements ElementCreatorFactory<FilterItemConfig> {

    /**
     * 与前端 {@code WorkshopFilterItemsProperty._filters} 字段同名。
     */
    public static final String TUPLES_KEY = "_filters";

    public static final String KEY_PROPERTY = "property";
    public static final String KEY_LABEL = "label";
    public static final String KEY_COMPONENT = "component";
    public static final String KEY_OPERATOR = "operator";

    /**
     * 控件形态候选列表的 key，供前端行编辑器渲染下拉框。
     */
    public static final String KEY_COMPONENTS = "components";

    /**
     * 算子候选列表的 key，同理。
     */
    public static final String KEY_OPERATORS = "operators";

    @Override
    public final String getTuplesKey() {
        return TUPLES_KEY;
    }

    @Override
    public ViewContent getViewContentType() {
        return ViewContent.WorkshopFilterItems;
    }

    /**
     * 把两个枚举的候选列表随 {@code _eprops.enum} 下发。
     *
     * <p>不把选项硬编码在前端组件里：枚举是线格式的唯一定义处（落盘值就是常量名），
     * 前端再抄一份就会在枚举增删时悄悄漂移。
     */
    @Override
    public void appendExternalJsonProp(IPropertyType propertyType, JSONObject biz) {
        biz.put(KEY_COMPONENTS, Option.toJson(componentOptions()));
        biz.put(KEY_OPERATORS, Option.toJson(operatorOptions()));
    }

    @Override
    public CMeta.ParsePostMCols<FilterItemConfig> parsePostMCols(//
                                                                  IPropertyType propertyType, IControlMsgHandler msgHandler, Context context,
                                                                  String keyColsMeta, JSONArray targetCols) {
        CMeta.ParsePostMCols<FilterItemConfig> parseResult = new CMeta.ParsePostMCols<>();
        if (targetCols == null) {
            // 前端没有提交过滤器列表：视为空列表，不算错误（filters 本身不是必填）
            return parseResult;
        }

        Map<String, Collection<Integer>> duplicateProps = Maps.newHashMap();
        for (int i = 0; i < targetCols.size(); i++) {
            final int tupleIndex = i;
            JSONObject tuple = targetCols.getJSONObject(i);

            // 逐行包一层 msgHandler，把错误挂到 "filters[3].property" 这样的字段路径上
            IControlMsgHandler rowMsgHandler = new BasicDelegateMsgHandler(msgHandler) {
                @Override
                public void addFieldError(Context context, String fieldName, String msg, Object... params) {
                    super.addFieldError(context, IFieldErrorHandler.joinField(keyColsMeta //
                            , Collections.singletonList(tupleIndex), fieldName), msg, params);
                }
            };

            FilterItemConfig item = this.createDefault(tuple);
            item.property = tuple.getString(KEY_PROPERTY);
            item.label = tuple.getString(KEY_LABEL);
            item.component = parseComponent(rowMsgHandler, context, tuple);
            item.operator = parseOperator(rowMsgHandler, context, tuple);

            if (StringUtils.isEmpty(item.property)) {
                rowMsgHandler.addFieldError(context, KEY_PROPERTY, ValidatorCommons.MSG_EMPTY_INPUT_ERROR);
            } else {
                Collection<Integer> sameProps = duplicateProps.get(item.property);
                if (sameProps == null) {
                    sameProps = Lists.newArrayList();
                    duplicateProps.put(item.property, sameProps);
                }
                sameProps.add(i);
            }
            if (StringUtils.isEmpty(item.label)) {
                rowMsgHandler.addFieldError(context, KEY_LABEL, ValidatorCommons.MSG_EMPTY_INPUT_ERROR);
            }

            parseResult.writerCols.add(item);
        }

        // 同一个属性在列表里出现两次没有意义：两条谓词会以「与」的关系施加到同一个属性上
        for (Map.Entry<String, Collection<Integer>> entry : duplicateProps.entrySet()) {
            if (entry.getValue().size() > 1) {
                for (Integer duplicateIndex : entry.getValue()) {
                    msgHandler.addFieldError(context, IFieldErrorHandler.joinField(keyColsMeta //
                            , Collections.singletonList(duplicateIndex), KEY_PROPERTY), IdentityName.MSG_ERROR_NAME_DUPLICATE);
                }
            }
        }

        parseResult.validateFaild = context.hasErrors();
        return parseResult;
    }

    private FilterItemConfig.FilterComponent //
    parseComponent(IControlMsgHandler msgHandler, Context context, JSONObject tuple) {
        String val = tuple.getString(KEY_COMPONENT);
        if (StringUtils.isEmpty(val)) {
            msgHandler.addFieldError(context, KEY_COMPONENT, ValidatorCommons.MSG_EMPTY_INPUT_ERROR);
            return null;
        }
        try {
            return FilterItemConfig.FilterComponent.valueOf(val);
        } catch (IllegalArgumentException e) {
            msgHandler.addFieldError(context, KEY_COMPONENT, "非法的控件形态：" + val);
            return null;
        }
    }

    /**
     * 算子是可选的：留空即由前端按控件形态推导，故空值不算错误。
     */
    private FilterItemConfig.FilterOperator //
    parseOperator(IControlMsgHandler msgHandler, Context context, JSONObject tuple) {
        String val = tuple.getString(KEY_OPERATOR);
        if (StringUtils.isEmpty(val)) {
            return null;
        }
        try {
            return FilterItemConfig.FilterOperator.valueOf(val);
        } catch (IllegalArgumentException e) {
            msgHandler.addFieldError(context, KEY_OPERATOR, "非法的算子：" + val);
            return null;
        }
    }

    @Override
    public FilterItemConfig createDefault(JSONObject targetCol) {
        return new FilterItemConfig();
    }

    @Override
    public FilterItemConfig create(JSONObject targetCol, BiConsumer<String, String> errorProcess) {
        FilterItemConfig item = this.createDefault(targetCol);
        item.property = targetCol.getString(KEY_PROPERTY);
        item.label = targetCol.getString(KEY_LABEL);
        String component = targetCol.getString(KEY_COMPONENT);
        if (!StringUtils.isEmpty(component)) {
            try {
                item.component = FilterItemConfig.FilterComponent.valueOf(component);
            } catch (IllegalArgumentException e) {
                errorProcess.accept(KEY_COMPONENT, "非法的控件形态：" + component);
            }
        }
        String operator = targetCol.getString(KEY_OPERATOR);
        if (!StringUtils.isEmpty(operator)) {
            try {
                item.operator = FilterItemConfig.FilterOperator.valueOf(operator);
            } catch (IllegalArgumentException e) {
                errorProcess.accept(KEY_OPERATOR, "非法的算子：" + operator);
            }
        }
        return item;
    }

    /**
     * 控件形态候选列表。
     */
    public static List<Option> componentOptions() {
        List<Option> opts = Lists.newArrayList();
        for (FilterItemConfig.FilterComponent component : FilterItemConfig.FilterComponent.values()) {
            opts.add(new Option(component.shortComment(), component.name()));
        }
        return opts;
    }

    /**
     * 算子候选列表。
     */
    public static List<Option> operatorOptions() {
        List<Option> opts = Lists.newArrayList();
        for (FilterItemConfig.FilterOperator operator : FilterItemConfig.FilterOperator.values()) {
            opts.add(new Option(operator.shortComment(), operator.name()));
        }
        return opts;
    }
}
