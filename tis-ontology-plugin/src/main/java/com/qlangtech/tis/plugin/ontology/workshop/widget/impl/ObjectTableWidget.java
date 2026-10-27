package com.qlangtech.tis.plugin.ontology.workshop.widget.impl;

import com.alibaba.citrus.turbine.Context;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.koubei.web.tag.pager.Pager;
import com.koubei.web.tag.pager.PaginationResult;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.ibatis.RowMap;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.impl.objtype.ObjectTypeBinding;
import com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopVariable;
import com.qlangtech.tis.plugin.ontology.workshop.model.variable.ObjectSetVariable;
import com.qlangtech.tis.plugin.ontology.workshop.widget.FullDisplayWidget;
import com.qlangtech.tis.plugin.ontology.workshop.widget.IWidgetColumnHost;
import com.qlangtech.tis.plugin.ontology.workshop.widget.NoPagenator;
import com.qlangtech.tis.plugin.ontology.workshop.widget.Pagenator;
import com.qlangtech.tis.plugin.ontology.workshop.widget.WidgetColumnConfig;
import com.qlangtech.tis.plugin.workshop.widget.IWorkshopWidget;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.util.AttrValMap;
import com.qlangtech.tis.util.IPluginContext;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

/**
 * P1 Widget：对象列表表格（Object Table）
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/9
 */
public class ObjectTableWidget extends FullDisplayWidget implements IWidgetColumnHost {

    public static final String KEY_OBJECT_SET_VAR = "objectSetVar";
    public static final String KEY_ACTIVE_OBJECT_VAR = "activeObjectVar";
    public static final String KEY_SELECTED_OBJECTS_VAR = "selectedObjectsVar";
    public static final String KEY_COLUMS_VAR = "columns";

    /**
     * 列排序用的两个**请求参数**（不是表单字段）：随 `get_data` 请求发在 query string 里，
     * 前端 `object-table.component.ts` 的 `sortParams()` 是同一条线上口径。
     */
    public static final String KEY_SORT_FIELD = "sortField";
    public static final String KEY_SORT_ORDER = "sortOrder";
    public static final String SORT_ORDER_ASC = "asc";
    public static final String SORT_ORDER_DESC = "desc";

    /**
     * 分页器字段名。⚠️ 它必须出现在 {@code get_data} 的**相关字段**白名单里
     * （见 {@link DescriptorImpl#httpProcess} 里 {@code createRelevantFieldsContext} 那个 lambda），
     * 否则 {@code widget.pagenator} 恒为 null —— 分页配置静默失效，不报错。
     */
    public static final String KEY_PAGENATOR = "pagenator";

    /**
     * 翻页用的**请求参数**（不是表单字段），同 {@link #KEY_SORT_FIELD}：发在 query string 里，
     * 前端 `object-table.component.ts` 的 `onPageIndexChange` 是同一条线上口径。
     */
    public static final String KEY_PAGE = "page";

    /**
     * {@link PaginationResult#getTotalCount()} 的「未统计」哨兵值：关闭分页时不该为一个用不上的
     * 标量多付一次全表 count，故用 -1 而不是 0 区分「没数」与「数出来是 0 条」。
     *
     * <p>它经 {@link DescriptorImpl#clampPage} 原样灌进 {@code Pager}，再被序列化出去 ——
     * 前端 `object-table.component.ts` 用 {@code totalCount >= 0} 把这个值挡掉。
     */
    public static final long NO_TOTAL_COUNT = -1L;

    /**
     * 对象集输入变量（options 动态供给）
     */
    @FormField(type = FormFieldType.SELECTABLE,ordinal = 20,validate = {Validator.require})
    public String objectSetVar;

    public ObjectSetVariable getObjectSetVariable() {
        return WorkshopVariable.loadDetail(objectSetVar);
    }

    /**
     * 列定义列表（由 SubForm 子表单结构化管理）
     */
    @FormField(type = FormFieldType.MULTI_DESCRIBLE_PLUGIN,desClazz = WidgetColumnConfig.class,ordinal = 21,validate = {Validator.require})
    public List<WidgetColumnConfig> columns;

    /**
     * 分页策略。多态（不指定 {@code type} → TIS 自动识别为嵌套子表单）：
     * <ul>
     *   <li>{@code NoPagenator}（displayName = {@code off}）：不开启分页，只显示第一页；</li>
     *   <li>{@code DefaultPagenator}：服务端分页，可配每页条数与「是否显示总数/页码」。</li>
     * </ul>
     *
     * <p>换了字段名（原 {@code pageSize: Integer}）而不是在旧字段旁挂一个开关：见 {@link Pagenator}
     * 的 javadoc —— 开关 + 载荷两个字段能拼出自相矛盾的状态。
     */
    @FormField(ordinal = 22,validate = {Validator.require})
    public Pagenator pagenator;

    /**
     * 当前行写回：点击某行后把该行对象写入此变量，供下游组件消费
     */
    @FormField(type = FormFieldType.SELECTABLE,ordinal = 23)
    public String activeObjectVar;

    /**
     * 多选集合写回：勾选集合变化后把选中行数组写入此变量，供下游组件消费
     */
    @FormField(type = FormFieldType.SELECTABLE,ordinal = 24)
    public String selectedObjectsVar;

    @Override
    public String getBoundObjectSetVar() {
        return this.objectSetVar;
    }

    @Override
    public List<WidgetColumnConfig> getColumnConfigs() {
        return this.columns;
    }

    @TISExtension
    public static class DescriptorImpl extends BaseWidgetDescriptor<IWorkshopWidget> {

        public DescriptorImpl() {
            super();
            // 数据源只能是「可查询的」对象集变量（ObjectSet Definition）；
            // 「外部写入」定义的对象集是选中行载体，填到这里会在取数时抛 illegal type。
            this.registerSelectOptions(KEY_OBJECT_SET_VAR, WidgetOptionHelper::getObjectSetSourceVariableOptions);
            this.registerSelectOptions(KEY_ACTIVE_OBJECT_VAR, WidgetOptionHelper::getObjectSetVariableOptions);
            this.registerSelectOptions(KEY_SELECTED_OBJECTS_VAR, WidgetOptionHelper::getObjectSetVariableOptions);

            // === 级联管道：objectSetVar 变化时，按该对象集变量的属性自动生成候选列 ===
            this.valueChangePipe(KEY_OBJECT_SET_VAR, KEY_COLUMS_VAR).render((pluginMeta, param) ->
                    WidgetColumnConfig.createFrom(
                            WidgetOptionHelper.getObjectPropertyMetas(param.getString(KEY_OBJECT_SET_VAR))));
        }

        @Override
        public String getDisplayName() {
            return "Object Table";
        }

        @Override
        public void httpProcess(IControlMsgHandler paramGetter, IPluginContext pluginContext, Context context) throws Exception {
            String methodType = paramGetter.getString("type");
            switch (methodType) {
                case "get_data": {
                    JSONObject postContent = pluginContext.getJSONPostContent();
                    List<String> relevantFields = Lists.newArrayList(KEY_OBJECT_SET_VAR, KEY_COLUMS_VAR);
                    AttrValMap.RelevantFieldsContextAttrValMap<ObjectTableWidget> relevantFieldsContextAttrs
                            = AttrValMap.createRelevantFieldsContext(paramGetter, context, postContent, (desc) -> {
                        // ⚠️ 这个白名单是**唯一**决定字段能不能被读到的开关：AdapterPluginFormProperties
                        // 只放行这里列出的 key，漏掉哪个，那个字段在下面就是 null，且不报错。
                        // （本改动之前 pageSize 就不在其中，所以旧的「每页显示条数」对服务端一直是个静默 no-op。）
                        return relevantFields;// List.of(KEY_OBJECT_SET_VAR, KEY_COLUMS_VAR, KEY_PAGENATOR);
                    });

                    if (!relevantFieldsContextAttrs.validate().isValid()) {
                        return;
                    }
                    relevantFields.add(KEY_PAGENATOR);
                    ObjectTableWidget widget = relevantFieldsContextAttrs.createPluginInstance();
                    ObjectSetVariable objSetVar = widget.getObjectSetVariable();
                    List<WidgetColumnConfig> cols = widget.getColumnConfigs();
                    ObjectTypeBinding.DataSourceQueryCriteria criteria = new ObjectTypeBinding.DataSourceQueryCriteria();
                    cols.forEach((col) -> criteria.addTargetCol(col.prop));
                    // 主键可能没有被配置成展示列，但下游（Button Group → OntologyAction）需要它。
                    // targetCols 是 Set，已经配置了该列时重复添加无副作用。
                    final String identityProp = objSetVar.getIdentityPropName();
                    if (StringUtils.isNotEmpty(identityProp)) {
                        criteria.addTargetCol(identityProp);
                    }

                    // 分页策略：off 只取第一页、且不必查总数（前端没有分页条，总数无处可显）。
                    Pagenator pagenator = widget.pagenator!= null?widget.pagenator: new NoPagenator();
                    final int pageSize = pagenator.resolvePageSize();
                    long totalCount = NO_TOTAL_COUNT;
                    int requestedPage = 1;
                    if (pagenator.pagingEnabled()) {
                        requestedPage = parsePage(paramGetter.getString(KEY_PAGE));
                        // 先 count 再切页，同 tis-console AppViewAction.doGetApps：不先拿到总数，
                        // 就无法判断请求的页码是否已越过末页（前端页码可能来自上一次查询的旧总数）。
                        totalCount = objSetVar.countQuery(criteria);
                    }
                    // 一个 Pager 既用来夹页码、又被 PaginationResult 序列化出去 —— 两者必须是同一份
                    // 状态，否则会出现「按夹取后的页码查了数据、却把请求页码回给前端」的错位
                    Pager pager = new Pager(null);
                    criteria.setPage(clampPage(pager, requestedPage, pageSize, totalCount));
                    criteria.setPageSize(pageSize);
                    // 列排序是**整表**语义：交给 SQL 排完整个对象集再切页，而不是把一页拉到前端排
                    applyOrderBy(paramGetter, criteria);
                    List<RowMap> rows = objSetVar.executeQuery(criteria);
                    if (StringUtils.isNotEmpty(identityProp)) {
                        for (RowMap row : rows) {
                            row.put(ObjectTypeBinding.KEY_ROW_IDENTITY, row.get(identityProp));
                        }
                    }
                    paramGetter.setBizResult(context, new PaginationResult(pager, rows));
                    return;
                }
                default:
                    throw new IllegalStateException("illegal methodType:" + methodType);
            }


        }

        /**
         * 读取可选的列排序参数并写进 criteria。
         *
         * <p><b>协议</b>：不带 sortField = 不排序（服务端默认顺序，SQL 里不会有 ORDER BY）；
         * 带了 sortField 则 sortOrder 取 asc / desc。
         *
         * <p><b>非法方向直接抛</b>：把 `descend`、`DESC ` 这类拼错静默当成升序，会得到一个方向相反
         * 却看不出错的结果（数据出来了、箭头也亮着，只是顺序反了）。故与同一 switch 里
         * `illegal methodType` 的处理口径保持一致 —— 让错误在入口处响。同理，sortField 若不是本
         * Widget 配置的列，会在 {@code criteria.setOrderBy} 那层被白名单挡下（见其注释，那是防注入的边界）。
         */
        private static void applyOrderBy(IControlMsgHandler paramGetter //
                , ObjectTypeBinding.DataSourceQueryCriteria criteria) {
            String sortField = paramGetter.getString(KEY_SORT_FIELD);
            if (StringUtils.isBlank(sortField)) {
                return;
            }
            criteria.setOrderBy(sortField, parseSortOrderAsc(paramGetter.getString(KEY_SORT_ORDER)));
        }

        /**
         * 排序方向取值解析。
         *
         * <p>单独成方法（而不是内联在上面的读取里）是为了够得着：它是这条协议里唯一「拼错了也照样
         * 出数据」的地方，值得被测试直接钉住，而 {@code applyOrderBy} 要读 {@link IControlMsgHandler}
         * 反而不好造。
         */
        static boolean parseSortOrderAsc(String sortOrder) {
            if (StringUtils.isBlank(sortOrder) || SORT_ORDER_ASC.equalsIgnoreCase(sortOrder)) {
                // 只给了列没给方向：按升序（前端两者总是一起发，这里只是宽容处理）
                return true;
            } else if (SORT_ORDER_DESC.equalsIgnoreCase(sortOrder)) {
                return false;
            } else {
                throw new IllegalArgumentException("illegal sortOrder:" + sortOrder + ", expect '"
                        + SORT_ORDER_ASC + "' or '" + SORT_ORDER_DESC + "'");
            }
        }

        /**
         * 请求页码解析。
         *
         * <p><b>缺省 = 第 1 页</b>（前端首次装配就不带 `page`）；但**非法值一律抛**，
         * 口径同 {@link #parseSortOrderAsc}：把 `page=0`、`page=-1` 静默当成第 1 页，
         * 会让一个算错页码的前端看起来完全正常 —— 用户以为在第 3 页，其实一直在看第 1 页。
         * 拼错了要立刻响。
         *
         * <p>页码从 1 起算，与 {@code BasicCriteria.page}、{@code nz-table} 的 {@code nzPageIndex}
         * 同口径（三者都是 1-based，少一层换算就少一处 off-by-one）。
         */
        static int parsePage(String page) {
            if (StringUtils.isBlank(page)) {
                return 1;
            }
            final int p;
            try {
                p = Integer.parseInt(page.trim());
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("illegal page:" + page);
            }
            if (p < 1) {
                throw new IllegalArgumentException("illegal page:" + page + ", expect an integer >= 1");
            }
            return p;
        }

        /**
         * 把 page / pageSize / totalCount 灌进调用方的分页器，并把页码夹到末页；返回实际生效的页码。
         *
         * <p><b>分页器由调用方持有</b>（而不是本方法内部 new 一个）：同一个实例既用来夹页码、
         * 又要交给 {@link PaginationResult} 序列化出去。若各建一个，就可能出现「按夹取后的页码
         * 查了数据、却把请求页码回给前端」的错位。本方法只负责配置 + 夹取。
         *
         * <p>夹取本身委托 {@link Pager#getCurPage()}，让「末页在哪、越界夹到哪」全仓只有一份实现
         * （此前是这里手写一遍、tis-console 的 {@code Pager} 里再写一遍，两份靠注释声明「口径一致」）。
         *
         * <p>为什么必须夹：前端手里的页码可能来自上一次查询的旧总数（例如数据被删掉了大半），
         * 于是请求一个已不存在的页。此时回空表会让用户看到一张空白表格且没有任何分页条可点
         * （总数变 0 → 无页码），看起来像「数据全没了」。
         *
         * <p><b>先落第 1 页的那一步不能省</b>，两个条件在 {@code Pager} 里都**不会**回第 1 页：
         * <ul>
         *   <li>{@code totalCount < 1}（含 {@link #NO_TOTAL_COUNT} 哨兵）：{@code Pager.getCurPage()}
         *       的 {@code getTotalPage() > 0} 前提不成立，于是不再夹取、把入参页码原样返回 ——
         *       那可是「关闭分页时也去请求第 9 页」；</li>
         *   <li>{@code pageSize < 1}：{@code getTotalPage()} 直接回 0，同上。</li>
         * </ul>
         * 「无数据 = 第 1 页」是我们这一侧的业务规则，不该顺手改掉 {@code Pager} 的公有语义去实现
         * （它有 9 个存量 action 消费者，其中 {@code OntologyAction} 从不设总数）。
         *
         * @param pager      调用方持有的分页器，本方法会往它上面写三样东西，并随后交给
         *                   {@link PaginationResult} 序列化
         * @param totalCount 总行数；&lt; 1 时（含 {@link #NO_TOTAL_COUNT} 哨兵）一律落到第 1 页
         * @return 夹取后实际生效的页码
         */
        static int clampPage(Pager pager, int page, int pageSize, long totalCount) {
            pager.setRowsPerPage(pageSize);
            // Pager 的计数是 int（不动它的公有签名，见 setTotalCount 的 javadoc）：这里饱和，
            // 而不是让 (int) 强转把超大总数截成一个可能为负的值 —— 那会让夹取方向整个反过来
            pager.setTotalCount(totalCount > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) totalCount);
            pager.setCurPage((totalCount < 1 || pageSize < 1) ? 1 : page);
            // 读取即夹取：getCurPage() 会把越界页码写回自己的 curPage（有副作用，见其 javadoc）
            return pager.getCurPage();
        }

        @Override
        public WorkShopWidgetType getWidgetType() {
            return WorkShopWidgetType.OBJECT_TABLE;
        }

        @Override
        protected Map<String, Object> appendExtractProps(Map<String, Object> props) {
            props.put("defaultSize", Map.of("height", 320));
            return props;
        }
    }
}
