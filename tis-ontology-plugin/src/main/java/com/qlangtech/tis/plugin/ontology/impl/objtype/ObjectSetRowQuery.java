package com.qlangtech.tis.plugin.ontology.impl.objtype;

import com.qlangtech.tis.ibatis.RowMap;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * 对象集的「一次性取行」查询：构造 criteria、夹取行数上限、补 {@link ObjectTypeBinding#KEY_ROW_IDENTITY}。
 *
 * <p>这段逻辑原先住在 tis-console 的 {@code WorkshopOntologyService#queryObjectSet}（走一条
 * {@code WorkshopOntologyAction} 的 REST 链路，由前端 {@code VariableService} 在算对象集变量时调用）。
 * 那条链路的两个硬伤是「行数由前端常量控制、排序发生在浏览器内存里」。现在取数收归各 widget 的
 * {@code get_data}，同一段 criteria 构造需要有<u>一个</u>出处 —— 就是本类。
 *
 * <p><b>为什么放在插件工程而不是 tis-solr/tis-plugin 核心层</b>：与 {@link DataSourceBinding} 同包，
 * 只有本插件（以及未来的历史对象类型插件）需要它；放进核心层等于为一次搬家重构整个核心模块。
 *
 * <p>本类只管「明细行」。聚合走 {@link ObjectTypeBinding#executeAggregation}（见
 * {@code PivotTableWidget#get_data}），两条路互不复用 —— 聚合不切页、不排序，硬凑一个公共骨架
 * 反而会让两边都变形。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/11/11
 * @see ObjectTypeBinding.DataSourceQueryCriteria
 */
public final class ObjectSetRowQuery {

    /**
     * 未指定上限时的默认行数。
     *
     * <p>500 这个值对应「卡片 / 图表一次展示」的量级：列表类 widget 自己还有更小的
     * {@code maxObjects}（50），这里是其余消费方的兜底。
     */
    public static final int DEFAULT_MAX_ROWS = 500;

    /**
     * 行数上限的硬顶。再大就不是「一次性取回」而是「把整张表搬进浏览器内存」——
     * 那正是这条链路被废弃的原因。
     */
    public static final int MAX_ROWS_LIMIT = 10_000;

    private ObjectSetRowQuery() {
    }

    /**
     * 把调用方想要的行数夹进 {@code [1, }{@link #MAX_ROWS_LIMIT}{@code ]}。
     *
     * <p><b>夹取而不抛</b>：与它的前身 {@code WorkshopOntologyService#clampLimit} 同一口径 ——
     * 行数上限是「展示策略」不是「请求合法性」，前端传 0 或负数时给默认值比让整张卡片报错更合理。
     * 超出硬顶则饱和（用户不会因为多填一个 0 就看不到数据）。
     */
    public static int clampMaxRows(Integer requested) {
        if (requested == null || requested < 1) {
            return DEFAULT_MAX_ROWS;
        }
        return Math.min(requested, MAX_ROWS_LIMIT);
    }

    /**
     * 构造「取前 N 行」的 criteria：targetCols + 主键 + {@code page=1} / {@code pageSize=maxRows}。
     *
     * <p>主键除供展示外还要单独补进 targetCols：{@code appendWhere} 的过滤白名单
     * （{@code targetCols ∪ filterCols}）与 {@code setOrderBy} 的判据（{@code col ∈ targetCols}）
     * 都只认这里声明过的列，而下游（Button Group → OntologyAction、排序工具条）都需要主键在场。
     *
     * <p>本方法<b>不施加任何过滤</b>：静态条件由 {@code ObjectSetVariable} 的三个取数方法内部施加，
     * 中继条件由调用方（{@code get_data} 分支）在拿到 criteria 之后、任何 count 之前施加。
     *
     * @param targetCols  本次要取的列；不能为空（空列表会拼出非法的 {@code select  from t}）
     * @param identityProp 主键属性名；null 表示该对象类型关闭了主键
     * @param maxRows     期望行数，内部会走 {@link #clampMaxRows}
     */
    public static ObjectTypeBinding.DataSourceQueryCriteria createCriteria(
            List<String> targetCols, String identityProp, int maxRows) {
        Objects.requireNonNull(targetCols, "targetCols can not be null");
        if (targetCols.isEmpty()) {
            throw new IllegalArgumentException("targetCols can not be empty");
        }
        ObjectTypeBinding.DataSourceQueryCriteria criteria = new ObjectTypeBinding.DataSourceQueryCriteria();
        for (String col : targetCols) {
            if (StringUtils.isNotBlank(col)) {
                criteria.addTargetCol(col);
            }
        }
        if (StringUtils.isNotEmpty(identityProp)) {
            // targetCols 是 Set：主键已经是展示列时重复添加无副作用
            criteria.addTargetCol(identityProp);
        }
        // 列表类查询不翻页，但 BasicCriteria 的 page / pageSize 不能为 null
        criteria.setPage(1);
        criteria.setPageSize(clampMaxRows(maxRows));
        return criteria;
    }

    /**
     * 直接对一个已绑定的数据源取行（不经 {@code ObjectSetVariable}）。
     *
     * <p>目前唯一的调用方是 Link Action 的目标下拉：它按 {@code targetObjectType} 取数，
     * 手上只有 {@code OntologyObjectType}，没有「对象集变量」这个中间层。
     *
     * <p>⚠️ 这条路径<b>没有</b>静态过滤可施加（过滤条件配在对象集变量上，不在对象类型上）。
     * 需要静态条件的场景请走 {@code ObjectSetVariable#fetchRows}。
     *
     * @param binding      对象类型绑定的数据源实现
     * @param identityProp 主键属性名；null 则不给行补 {@link ObjectTypeBinding#KEY_ROW_IDENTITY}
     * @param targetCols   要取的列（不能为空）
     * @param maxRows      期望行数
     * @return 已补好 {@code __rid} 的行
     */
    public static List<RowMap> fetch(ObjectTypeBinding binding, String identityProp,
                                     List<String> targetCols, int maxRows) {
        Objects.requireNonNull(binding, "binding can not be null");
        ObjectTypeBinding.DataSourceQueryCriteria criteria = createCriteria(targetCols, identityProp, maxRows);
        return decorateIdentity(binding.executeQuery(criteria), identityProp);
    }

    /**
     * 给每行补 {@link ObjectTypeBinding#KEY_ROW_IDENTITY}。
     *
     * <p>前端不猜主键列名，交互侧（Button Group → OntologyAction、多选写回）只认 {@code __rid}。
     * 主键为 null（对象类型关闭了主键）时原样返回 —— 补一个 null 值只会让前端多一个恒空的键。
     */
    public static List<RowMap> decorateIdentity(List<RowMap> rows, String identityProp) {
        if (StringUtils.isEmpty(identityProp) || rows == null) {
            return rows;
        }
        for (RowMap row : rows) {
            row.put(ObjectTypeBinding.KEY_ROW_IDENTITY, row.get(identityProp));
        }
        return rows;
    }

    /**
     * 对象类型全部属性名 —— 「取全属性」调用方（下拉 / 时间轴 / 卡片）的公共 shortcut。
     */
    public static List<String> allColNames(List<com.qlangtech.tis.plugin.ontology.OntologyProperty> cols) {
        List<String> names = new ArrayList<>(cols.size());
        for (com.qlangtech.tis.plugin.ontology.OntologyProperty col : cols) {
            names.add(col.getName());
        }
        return names;
    }
}
