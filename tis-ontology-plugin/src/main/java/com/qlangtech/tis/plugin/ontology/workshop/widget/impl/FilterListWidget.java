package com.qlangtech.tis.plugin.ontology.workshop.widget.impl;

import com.alibaba.citrus.turbine.Context;
import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.impl.OntologyPluginMeta;
import com.qlangtech.tis.plugin.ontology.workshop.widget.FilterItemConfig;
import com.qlangtech.tis.plugin.ontology.workshop.widget.WorkshopWidget;
import com.qlangtech.tis.plugin.workshop.widget.IWorkshopWidget;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.util.AttrValMap;
import com.qlangtech.tis.util.DescribableJSON;
import com.qlangtech.tis.util.IPluginContext;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * P0 Widget：过滤列表（Filter List）
 * <a href="https://gitee.com/qlangtech/palantir-study/blob/master/foundry/workshop/detail/04-widgets/widgets-filter-list.md">...</a>
 * <p>
 * 组合多种过滤器组件（关键词、单选、多选、日期选择），统一输出过滤条件。
 *
 * <h3>为什么需要 {@link #objectSetVar}</h3>
 * 本 Widget 有两个用途不同的对象集概念，缺一不可：
 * <ul>
 *   <li>{@code objectSetVar} 决定<b>过滤谁</b>——逐行过滤项的目标属性候选随它级联刷新，
 *       写出的谓词也要带上它的 {@code objectType} 供下游判定能否施加；</li>
 *   <li>{@link #filterOutputVar} 决定<b>写去哪</b>——一个 OBJECT_SET_FILTER 类型变量，
 *       由下游的对象集变量通过 {@code ObjectSetDefinitionConfig.filterVar} 引用。</li>
 * </ul>
 *
 * <p><b>单选 / 多选的候选项与本 Widget 的 {@code objectSetVar} 无关</b>：候选项来源是每个
 * 过滤项自带的嵌套配置（{@code AbstractSelectComponent#optionsSource}），既可以手填，
 * 也可以绑定<b>另一个</b>对象集变量。候选项由
 * {@code AbstractOpsSource#createOptions()} 算好，随 item 信封的 {@code appendVals.options}
 * 一并下发。二者在页面上是独立可变的：同一个过滤变量可以被多条链路复用。
 *
 * <h3>候选项的两条下发链路</h3>
 * <ol>
 *   <li><b>主链路（module GET）</b>：序列化 module 时自动带上 {@code appendVals.options}，
 *       页面首次打开就位；</li>
 *   <li><b>补偿链路（{@link #HTTP_METHOD_GET_OPTIONS}）</b>：配置面板每次编辑都会把 widget 的
 *       {@code filters} 重新投影成 {@code {impl, displayName, vals}}（见前端
 *       {@code toItemJson}），{@code appendVals} 作为 GET-only 载荷就此丢失。此时前端补发一次
 *       本端点，服务端用 {@link DescribableJSON} 重新序列化一份完整 item 返回。</li>
 * </ol>
 * 两条链路的选项都由同一个出口产出（{@code AbstractOpsSource#createPayloadProps()}），
 * 所以键名不可能分叉。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/12
 */
public class FilterListWidget extends WorkshopWidget {

    public static final String KEY_OBJECT_SET_VAR = "objectSetVar";
    public static final String KEY_FILTERS = "filters";
    public static final String KEY_FILTER_OUTPUT_VAR = "filterOutputVar";

    /**
     * {@code httpProcess} 的方法名：配置变更后重新解析候选项。
     *
     * <p>前端 {@code filter-list.component.ts} 的 {@code applyHttpRequest(..., 'get_options', ...)}
     * 是同一条线上口径，改名必须两侧同时改（拼错的 {@code type} 不报错，只会在下面的分派判断上抛异常）。
     *
     * <p>⚠️ 这不是候选项的主链路：module GET 已通过 {@code appendVals.options} 下发一次。
     * 本端点只为「配置面板刚改过、{@code appendVals} 被前端 {@code Item.project()} 丢掉」这一幕兜底。
     */
    public static final String HTTP_METHOD_GET_OPTIONS = "get_options";

    /**
     * 过滤作用的对象集变量：决定候选项来源与写出谓词的 objectType。
     */
    @FormField(type = FormFieldType.SELECTABLE,ordinal = 20,validate = {Validator.require})
    public String objectSetVar;

    /**
     * 过滤项列表。
     *
     * <p>非必填：空列表表示「不过滤」，是页面刚建好、过滤项还没配时的合法中间态
     * （归一与消费两侧对 null / 空列表同样按「不过滤」处理）。
     */
    @FormField(type = FormFieldType.MULTI_DESCRIBLE_PLUGIN,desClazz = FilterItemConfig.class,ordinal = 21)
    public List<FilterItemConfig> filters;

    /**
     * 过滤结果写回：组合出的过滤条件写入该「对象集合过滤器」变量，供下游组件作为过滤源
     */
    @FormField(type = FormFieldType.SELECTABLE,ordinal = 22)
    public String filterOutputVar;

    @TISExtension
    public static class DescriptorImpl extends BaseWidgetDescriptor<IWorkshopWidget> {

        public DescriptorImpl() {
            super();
            this.registerSelectOptions(KEY_OBJECT_SET_VAR, WidgetOptionHelper::getObjectSetVariableOptions);
            this.registerSelectOptions(KEY_FILTER_OUTPUT_VAR,
                    WidgetOptionHelper::getObjectSetFilterVariableOptions);

            // === 级联管道：objectSetVar 变化时，逐行刷新过滤项的属性候选 ===
            // 落点写成 "filters[].property"：[] 是「作用于列表每一行」的约定标记，
            // 只在宿主这一侧解析，元素类 FilterItemConfig 不认识它。
            this.valueChangePipe(KEY_OBJECT_SET_VAR, KEY_FILTERS + "[]." + FilterItemConfig.KEY_PROPERTY)
                    .render((pluginMeta, param) -> {
                        String selectedVar = param.getString(KEY_OBJECT_SET_VAR);
                        if (selectedVar == null) {
                            return Collections.emptyList();
                        }
                        return WidgetOptionHelper.getObjectPropertyOptions(
                                selectedVar, OntologyPluginMeta.createPluginMeta(pluginMeta));
                    });
        }

        /**
         * 配置变更后的候选项补偿取数。
         *
         * <p>返回体是<b>一份完整的 item 信封</b>（与 module GET 的单个 widget 同构），
         * 前端只读其中的 {@code vals.filters[…].vals.component.vals.optionsSource.appendVals}，
         * 不复用其余字段 —— 本端点不回灌配置。
         */
        @Override
        public void httpProcess(IControlMsgHandler paramGetter, IPluginContext pluginContext, Context context)
                throws Exception {
            String methodType = paramGetter.getString("type");
            if (!HTTP_METHOD_GET_OPTIONS.equals(methodType)) {
                throw new IllegalStateException("illegal methodType:" + methodType);
            }
            JSONObject postContent = Objects.requireNonNull(pluginContext.getJSONPostContent(),
                    "postContent can not be null");
            // ⚠️ 这个白名单是**唯一**决定字段能不能被读到的开关：AdapterPluginFormProperties 只放行
            // 这里列出的 key，漏掉 filters，下面拿到的 widget.filters 就是 null，且不报错。
            // objectSetVar 本身不参与候选项解析（候选项来源自带变量绑定），但它是本 Widget 的必填字段，
            // 漏掉它会让 validate() 直接不过。
            AttrValMap.RelevantFieldsContextAttrValMap<FilterListWidget> attrs
                    = AttrValMap.createRelevantFieldsContext(paramGetter, context, postContent,
                    (desc) -> List.of(KEY_OBJECT_SET_VAR, KEY_FILTERS));
            if (!attrs.validate().isValid()) {
                // 校验失败的字段级错误已经写进 context，前端按 errorfields 内联成红字
                return;
            }
            paramGetter.setBizResult(context, getItemJson(attrs.createPluginInstance()));
        }

        /**
         * 把 widget 序列化成前端 item 信封。
         *
         * <pre>
         * { impl, displayName, vals: {
         *     …, filters: [
         *       { impl, displayName, vals: {
         *           …, component: { impl, displayName, vals: {
         *               optionsSource: { impl, displayName, vals: {…}, appendVals: {options: [{label, val}]} }
         *           }}
         *       }}
         *     ]
         * }}
         * </pre>
         *
         * <p>候选项的序列化<b>不在这里做</b>：
         * {@link com.qlangtech.tis.plugin.ontology.workshop.widget.component.AbstractOpsSource}
         * 实现了 {@link DescribableJSON.AppendPayloadAttribute}，{@code getItemJson()} 遍历嵌套
         * describable 时会为它自动写上 {@code appendVals} —— 与 module GET 完全同一个出口。
         * 显式传 descriptor 而不是用 {@code new DescribableJSON(widget)}：省掉
         * {@code instance.getDescriptor()} 里的 {@code TIS.get()} 反查（也让单测不依赖运行期）。
         */
        JSONObject getItemJson(FilterListWidget widget) throws Exception {
            return new DescribableJSON<IWorkshopWidget>(widget, this).getItemJson();
        }

        @Override
        public String getDisplayName() {
            return "Filter List";
        }

        @Override
        public WorkShopWidgetType getWidgetType() {
            return WorkShopWidgetType.FILTER_LIST;
        }
    }
}
