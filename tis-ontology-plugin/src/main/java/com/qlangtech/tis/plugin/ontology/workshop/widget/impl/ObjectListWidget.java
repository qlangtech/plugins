package com.qlangtech.tis.plugin.ontology.workshop.widget.impl;

import com.alibaba.citrus.turbine.Context;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.koubei.web.tag.pager.Pager;
import com.koubei.web.tag.pager.PaginationResult;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.ibatis.RowMap;
import com.qlangtech.tis.manage.common.Option;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.impl.OntologyPluginMeta;
import com.qlangtech.tis.plugin.ontology.impl.objtype.ObjectSetRowQuery;
import com.qlangtech.tis.plugin.ontology.impl.objtype.ObjectTypeBinding;
import com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopVariable;
import com.qlangtech.tis.plugin.ontology.workshop.model.variable.ObjectSetVariable;
import com.qlangtech.tis.plugin.ontology.workshop.widget.DefaultObjectSelection;
import com.qlangtech.tis.runtime.module.action.IParamGetter;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import com.qlangtech.tis.util.AttrValMap;
import com.qlangtech.tis.util.IPluginContext;
import com.qlangtech.tis.util.UploadPluginMeta;
import com.qlangtech.tis.plugin.ontology.workshop.widget.DefaultListSorting;
import com.qlangtech.tis.plugin.ontology.workshop.widget.IWidgetColumnHost;
import com.qlangtech.tis.plugin.ontology.workshop.widget.ListSorting;
import com.qlangtech.tis.plugin.ontology.workshop.widget.NoListSorting;
import com.qlangtech.tis.plugin.ontology.workshop.widget.NoObjectSelection;
import com.qlangtech.tis.plugin.ontology.workshop.widget.ObjectListDisplay;
import com.qlangtech.tis.plugin.ontology.workshop.widget.ObjectSelection;
import com.qlangtech.tis.plugin.ontology.workshop.widget.WidgetActionConfig;
import com.qlangtech.tis.plugin.ontology.workshop.widget.WidgetColumnConfig;
import com.qlangtech.tis.plugin.ontology.workshop.widget.WorkshopWidget;
import com.qlangtech.tis.plugin.workshop.widget.IWorkshopWidget;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;


/**
 * P0 Widget：对象列表卡片（Object List）
 * <a href="https://gitee.com/qlangtech/palantir-study/blob/master/foundry/workshop/detail/04-widgets/widgets-object-list.md">...</a>
 * <p>
 * 以卡片网格形式展示对象集合，支持响应式布局和点击选择输出变量。
 * <p>
 * 字段分组：<b>数据来源</b>（{@code objectSetVar} + 级联出的 {@code titleProperty} / {@code listSorting.sortField}）、
 * <b>内容</b>（{@code cardFields}、{@code hideNullProperties}）、
 * <b>排序策略</b>（{@code listSorting}，{@link ListSorting} 的 off / Default 多态）、
 * <b>展示形态</b>（{@code display}，列表/网格二选一）、
 * <b>选择与交互</b>（{@code activeObjectVar} 单选写回、{@code objectSelection} 多选策略、
 * {@code cardActions} 行内动作）、<b>外观</b>（{@code showObjectCount}、{@code emptyStateMessage}）。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/12
 */
public class ObjectListWidget extends WorkshopWidget implements IWidgetColumnHost {

    public static final String KEY_OBJECT_SET_VAR = "objectSetVar";
    public static final String KEY_ACTIVE_OBJECT_VAR = "activeObjectVar";
    public static final String KEY_TITLE_PROPERTY = "titleProperty";
    public static final String KEY_CARD_FIELDS = "cardFields";
    public static final String KEY_DISPLAY = "display";
    public static final String KEY_CARD_ACTIONS = "cardActions";
    public static final String KEY_LIST_SORTING = "listSorting";
    public static final String KEY_HIDE_NULL_PROPERTIES = "hideNullProperties";
    public static final String KEY_OBJECT_SELECTION = "objectSelection";
    public static final String KEY_SHOW_OBJECT_COUNT = "showObjectCount";
    public static final String KEY_EMPTY_STATE_MESSAGE = "emptyStateMessage";
    public static final String KEY_MAX_OBJECTS = "maxObjects";

    /**
     * {@code maxObjects} 的默认值。
     *
     * <p>50 是「一屏卡片能看过来」的量级。此前的实现一次拉 1000 行（{@code OBJECT_SET_QUERY_LIMIT}）
     * 并全部渲染，既拉长了首屏也把排序推到了浏览器内存里。
     */
    public static final int DEFAULT_MAX_OBJECTS = 50;

    /**
     * {@code maxObjects} 的硬顶。再大就退回「把整个对象集搬进浏览器」的旧形态，
     * 与「取数在服务端完成一次」的目标相悖。
     */
    public static final int MAX_OBJECTS_LIMIT = 1000;

    /**
     * 列表数据来源：当前模块中的对象集合变量（options 由 Descriptor 动态供给）
     */
    @FormField(type = FormFieldType.SELECTABLE, ordinal = 10, validate = {Validator.require})
    public String objectSetVar;

    /**
     * 卡片标题属性。
     * <p>
     * 选项由 {@code objectSetVar} 级联得出（见 Descriptor 中的 {@code valueChangePipe}），
     * 因此必须是<b>同层普通字段</b>，不能下沉进 {@code cardFields} 子表单。
     */
    @FormField(type = FormFieldType.SELECTABLE, ordinal = 11, validate = {Validator.require})
    public String titleProperty;

    /**
     * 卡片正文展示的字段列表（由子表单结构化管理，非手写 JSON）
     */
    @FormField(type = FormFieldType.MULTI_DESCRIBLE_PLUGIN, desClazz = WidgetColumnConfig.class, ordinal = 12, validate = {Validator.require})
    public List<WidgetColumnConfig> cardFields;

    /**
     * 展示形态：列表 / 网格。
     * <p>
     * 不指定 {@code type} —— 字段类型声明为 {@link ObjectListDisplay} 子类时，
     * TIS 自动识别为嵌套子表单（CLAUDE.md 原则一）。选 Grid 只渲染 Grid 的字段，
     * 选 List 只渲染 List 的字段；这正是当初不做「mode 枚举 + 可空载荷字段」的原因。
     * <p>
     * 默认项由同名 {@code .json} 里的 {@code "dftVal": "Grid"} 逐字匹配子类
     * {@code getDisplayName()} 决定（见 {@link ObjectListDisplay} 类注释）。
     */
    @FormField(ordinal = 13)
    public ObjectListDisplay display;

    /**
     * 当前选中项写回：点击卡片后写入该变量，供下游组件消费
     */
    @FormField(type = FormFieldType.SELECTABLE, ordinal = 14)
    public String activeObjectVar;

    /**
     * 卡片行内动作按钮（点击后触发本体 Action），默认收起在「高级」分组
     */
    @FormField(type = FormFieldType.MULTI_DESCRIBLE_PLUGIN, desClazz = WidgetActionConfig.class, ordinal = 15)
    public List<WidgetActionConfig> cardActions;

    /**
     * 排序策略。多态（不指定 {@code type} → TIS 自动识别为嵌套子表单）：
     * <ul>
     *   <li>{@link NoListSorting}（displayName = {@code off}）：不排序，保持对象集原始顺序；</li>
     *   <li>{@link DefaultListSorting}：按指定属性排序，且在列表上方渲染排序字段 / 方向选择器。</li>
     * </ul>
     *
     * <p>用一个多态字段取代原先的 {@code defaultSortField} + {@code defaultSortOrder} +
     * {@code enableUserSorting} 三个字段：那三者能拼出自相矛盾的状态（见 {@link ListSorting} 的 javadoc）。
     */
    @FormField(ordinal = 16, validate = {Validator.require})
    public ListSorting listSorting = new NoListSorting();

    /**
     * 单次最多取回多少条对象。取数在服务端的 {@code get_data} 里按这个上限切页，
     * 排序也交给 SQL 在<b>整个</b>对象集上完成 —— 而不是「取一批回来再在浏览器里排」。
     *
     * <p>用 {@code Integer} 包装类型而非 {@code int}：XStream 反序列化不走构造器也不执行字段初值，
     * 存量落盘 XML 里没有这个元素时 {@code int} 会得到 0（静默变成「一条都取不到」），
     * {@code Integer} 至少能在这里看出是「没配」，由 {@link #resolveMaxObjects()} 兜底。
     */
    @FormField(type = FormFieldType.INT_NUMBER, ordinal = 17, validate = {Validator.integer})
    public Integer maxObjects = DEFAULT_MAX_OBJECTS;

    /**
     * 属性值为空（null / 空串）的字段是否按对象隐藏
     */
    @FormField(type = FormFieldType.ENUM, ordinal = 19)
    public Boolean hideNullProperties = Boolean.FALSE;

    /**
     * 选择策略。多态（不指定 {@code type} → TIS 自动识别为嵌套子表单）：
     * <ul>
     *   <li>{@link NoObjectSelection}（displayName = {@code off}）：只能单选，点击写回 {@link #activeObjectVar}；</li>
     *   <li>{@link DefaultObjectSelection}：每个对象前出现勾选框，选中集合写回指定变量。</li>
     * </ul>
     *
     * <p>用一个多态字段取代原先的 {@code enableMultiSelect} + {@code selectedObjectsVar} 两个字段。
     * 注意<b>单选始终可用</b>（{@link #activeObjectVar} 与本字段无关）。
     */
    @FormField(ordinal = 20, validate = {Validator.require})
    public ObjectSelection objectSelection = new NoObjectSelection();

    /**
     * 是否在标题下方显示对象总数
     */
    @FormField(type = FormFieldType.ENUM, ordinal = 22)
    public Boolean showObjectCount = Boolean.FALSE;

    /**
     * 无对象时的自定义空态文案；空则前端回落「暂无数据」
     */
    @FormField(type = FormFieldType.INPUTTEXT, ordinal = 23)
    public String emptyStateMessage;

    @Override
    public String getBoundObjectSetVar() {
        return this.objectSetVar;
    }

    /**
     * 生效的条数上限：null / 小于 1 → {@link #DEFAULT_MAX_OBJECTS}，超过 {@link #MAX_OBJECTS_LIMIT}
     * 则饱和（夹取而非抛，同 {@code ObjectSetRowQuery#clampMaxRows} 的口径）。
     */
    public int resolveMaxObjects() {
        if (this.maxObjects == null || this.maxObjects < 1) {
            return DEFAULT_MAX_OBJECTS;
        }
        return Math.min(this.maxObjects, MAX_OBJECTS_LIMIT);
    }

    /**
     * {@code get_data} 的数据源载体；必须由 {@code validate()} 通过后调用，否则变量名可能为空
     */
    public ObjectSetVariable getObjectSetVariable() {
        return WorkshopVariable.loadDetail(this.objectSetVar);
    }

    @Override
    public List<WidgetColumnConfig> getColumnConfigs() {
        return this.cardFields;
    }

    /**
     * XStream 反序列化不走构造器、也不执行字段初值：本字段上线前落盘的 XML 里没有
     * {@code <listSorting>} 元素时，不兜底会得到 {@code null}，前端归一化器与后端
     * {@code sortingEnabled()} 调用都要处理空值。
     */
    private Object readResolve() {
        if (this.listSorting == null) {
            this.listSorting = new NoListSorting();
        }
        if (this.objectSelection == null) {
            this.objectSelection = new NoObjectSelection();
        }
        return this;
    }

    @TISExtension
    public static class DescriptorImpl extends BaseWidgetDescriptor<IWorkshopWidget> {

        public DescriptorImpl() {
            super();
            // 数据源只能是「可查询的」对象集变量（ObjectSet Definition）；
            // 「函数计算 / 外部写入」定义的对象集在 get_data 里会抛 illegal type，不该出现在数据源下拉里。
            this.registerSelectOptions(KEY_OBJECT_SET_VAR, WidgetOptionHelper::getObjectSetSourceVariableOptions);
            // 写回载体则允许任意 OBJECT_SET（外部写入型正是「接收选中项」的典型用法）
            this.registerSelectOptions(KEY_ACTIVE_OBJECT_VAR, WidgetOptionHelper::getObjectSetVariableOptions);

            // === 级联属性字段：初始为空（未选 objectSetVar 时无选项）===
            //
            // listSorting.sortField 不在这里注册 —— 它已经在 DefaultListSorting.DefaultDescriptor
            // 的构造器里登记了空列表兜底（多态字段下沉后，注册必须跟着字段走）。
            this.registerSelectOptions(KEY_TITLE_PROPERTY, Collections::emptyList);

            // === 级联管道：objectSetVar 变化时，同时更新 titleProperty 的候选属性、
            //     cardFields 的候选列，以及排序子表单里的 sortField 候选 ===
            //
            // 三者同源于 objectSetVar，但需要的内容形态不同（前两者与 sortField 是属性下拉选项，
            // cardFields 是列配置行），故用逐字段渲染，而不能共用一个广播结果。
            //
            // listSorting.sortField 是「点分目标」：RootFormProperties#getInstancePropsJson 会把
            // toField 原样写成宿主 vals 上的 "$pipe_field$listSorting.sortField"，读侧下钻子表单时
            // 按 anchor path 前缀找到它。点分键必须同时出现在 valueChangePipe(...) 与下面的
            // render(...) 里 —— 少了任一处都会在运行期抛异常或静默失效。
            //
            // 注意 getObjectPropertyOptions 与 cardFields 走的 getObjectPropertyMetas 对 FunctionConfig
            // 型对象集的口径目前不一致：前者内部宽 catch 会静默降级为空候选，后者会直接抛错。
            // 已就此在 WidgetOptionHelper 上留了说明；本次有意不改动前者。
            this.valueChangePipe(KEY_OBJECT_SET_VAR, KEY_TITLE_PROPERTY, KEY_CARD_FIELDS
                            , KEY_LIST_SORTING + "." + DefaultListSorting.KEY_SORT_FIELD)
                    .render(KEY_TITLE_PROPERTY, DescriptorImpl::objectPropertyOptions)
                    .render(KEY_CARD_FIELDS, (pluginMeta, params) -> WidgetColumnConfig.createFrom(
                            WidgetOptionHelper.getObjectPropertyMetas(params.getString(KEY_OBJECT_SET_VAR))))
                    .render(KEY_LIST_SORTING + "." + DefaultListSorting.KEY_SORT_FIELD
                            , DescriptorImpl::objectPropertyOptions);
        }

        /**
         * 服务端取数。
         *
         * <p>协议与该 widget 的前端 {@code object-list.component.ts} 的 {@code fetchRows()} 一一对应：
         * 请求体是整棵 widget 的表单投影（{@code resolveProjectionItem}），query string 带
         * {@code sortField} / {@code sortOrder}（排序工具条）与 {@code filters}（中继条件）。
         *
         * <p><b>排序</b>在 SQL 里对整个对象集生效（{@code criteria.setOrderBy}），不是「取 50 行再排」；
         * <b>条数</b>由 {@code maxObjects} 决定；{@code showObjectCount} 打开时才多付一次 count。
         */
        @Override
        public void httpProcess(IControlMsgHandler paramGetter, IPluginContext pluginContext, Context context) throws Exception {
            String methodType = paramGetter.getString("type");
            switch (methodType) {
                case "get_data": {
                    JSONObject postContent = pluginContext.getJSONPostContent();
                    List<String> relevantFields = Lists.newArrayList(KEY_OBJECT_SET_VAR, KEY_TITLE_PROPERTY, KEY_CARD_FIELDS);
                    AttrValMap.RelevantFieldsContextAttrValMap<ObjectListWidget> relevantFieldsContextAttrs
                            = AttrValMap.createRelevantFieldsContext(paramGetter, context, postContent, (desc) -> {
                        // ⚠️ 漏列某个字段 = 它在下面恒为 null，且不报错（见 AttrValMap 的契约）
                        return relevantFields;
                    });

                    if (!relevantFieldsContextAttrs.validate().isValid()) {
                        return;
                    }
                    // ⚠️ 必须在 createPluginInstance() 之前追加（getKVTuples() 惰性求值，实例建好后再加等于没加）；
                    // 放在 validate() 之后是因为 listSorting 带 Validator.require，而存量落盘 XML 里没有
                    // <maxObjects> 元素 —— 一起参与校验会让老 widget 整体取数失败。二者都有字段级兜底。
                    relevantFields.add(KEY_LIST_SORTING);
                    relevantFields.add(KEY_MAX_OBJECTS);
                    // 只决定「要不要多付一次 count」，不参与校验；缺省 false 与字段初值一致
                    relevantFields.add(KEY_SHOW_OBJECT_COUNT);

                    ObjectListWidget widget = relevantFieldsContextAttrs.createPluginInstance();
                    ObjectSetVariable objSetVar = widget.getObjectSetVariable();

                    // --- targetCols：展示列 + 标题列 + 主键 + 排序字段 ---
                    // 标题列与排序字段必须进 targetCols：前者要在卡片上显示，后者是
                    // criteria.setOrderBy 的白名单判据（col ∈ targetCols），漏了直接 500。
                    List<String> targetCols = new ArrayList<>();
                    for (WidgetColumnConfig col : widget.cardFields) {
                        if (StringUtils.isNotBlank(col.prop)) {
                            targetCols.add(col.prop);
                        }
                    }
                    if (StringUtils.isNotBlank(widget.titleProperty)) {
                        targetCols.add(widget.titleProperty);
                    }
                    final String identityProp = objSetVar.getIdentityPropName();
                    final ListSorting listSorting = widget.listSorting != null ? widget.listSorting : new NoListSorting();
                    final String configuredSortField = listSorting.sortingEnabled() ? listSorting.resolveSortField() : null;
                    if (StringUtils.isNotBlank(configuredSortField)) {
                        targetCols.add(configuredSortField);
                    }

                    ObjectTypeBinding.DataSourceQueryCriteria criteria
                            = ObjectSetRowQuery.createCriteria(targetCols, identityProp, widget.resolveMaxObjects());

                    // 排序：请求参数优先（用户在排序工具条上的选择），否则配置里的默认值。
                    // ⚠️ 请求的 sortField 也可能不在 targetCols 里 —— 那是 criteria.setOrderBy 的白名单边界，
                    // 会抛而不是静默不排。
                    String sortField = StringUtils.trimToNull(paramGetter.getString(ObjectTableWidget.KEY_SORT_FIELD));
                    if (sortField == null) {
                        sortField = configuredSortField;
                    }
                    if (StringUtils.isNotBlank(sortField)) {
                        String sortOrder = StringUtils.trimToNull(paramGetter.getString(ObjectTableWidget.KEY_SORT_ORDER));
                        boolean asc = sortOrder != null
                                ? ObjectTableWidget.DescriptorImpl.parseSortOrderAsc(sortOrder)
                                : listSorting.resolveSortOrder() != ListSorting.SortOrder.desc;
                        criteria.setOrderBy(sortField, asc);
                    }

                    // 中继条件必须早于 countQuery（否则行数按过滤后算、总数按全表算，末页永远翻不到底）。
                    // 静态条件不在这里读 —— ObjectSetVariable 的三个取数方法内部自动施加。
                    ObjectTableWidget.DescriptorImpl.applyRuntimeFilters(paramGetter, objSetVar, criteria);

                    // 总数只在「显示对象数量」打开时才算：关掉时前端没有地方显示它，多打一次全表 count 不值得
                    long totalCount = Boolean.TRUE.equals(widget.showObjectCount)
                            ? objSetVar.countQuery(criteria) : ObjectTableWidget.NO_TOTAL_COUNT;
                    Pager pager = new Pager(null);
                    criteria.setPage(ObjectTableWidget.DescriptorImpl.clampPage(pager, 1, criteria.getPageSize(), totalCount));

                    List<RowMap> rows = objSetVar.executeQuery(criteria);
                    ObjectSetRowQuery.decorateIdentity(rows, identityProp);
                    paramGetter.setBizResult(context, new PaginationResult(pager, rows));
                    return;
                }
                default:
                    throw new IllegalStateException("illegal methodType:" + methodType);
            }
        }


        public boolean validateMaxObjects(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            //  return super.validateCustomerField(msgHandler, context, fieldName, value);
            if (StringUtils.isEmpty(value)) {
                return true;
            }
            int max = Integer.parseInt(value);
            if (max > MAX_OBJECTS_LIMIT) {
                msgHandler.addFieldError(context, fieldName, "不能超过最大允许数："+ MAX_OBJECTS_LIMIT);
                return false;
            }
            return true;
        }

        /**
         * 按当前选中的对象集变量取属性清单；未选时给空候选（前端下拉留空）
         */
        private static List<? extends Option> objectPropertyOptions(UploadPluginMeta pluginMeta, IParamGetter params) {
            String selectedVar = params.getString(KEY_OBJECT_SET_VAR);
            if (selectedVar == null) {
                return Collections.emptyList();
            }
            return WidgetOptionHelper.getObjectPropertyOptions(
                    selectedVar, OntologyPluginMeta.createPluginMeta(pluginMeta));
        }

        @Override
        public String getDisplayName() {
            return "Object List";
        }

        @Override
        public WorkShopWidgetType getWidgetType() {
            return WorkShopWidgetType.OBJECT_LIST;
        }

        @Override
        protected Map<String, Object> appendExtractProps(Map<String, Object> props) {
            props.put("defaultSize", Map.of("height", 400));
            return props;
        }
    }
}