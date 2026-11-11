package com.qlangtech.tis.plugin.ontology.workshop.model.variable;

import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.ibatis.RowMap;
import com.qlangtech.tis.lang.TisException;
import com.qlangtech.tis.plugin.ontology.OntologyObjectType;
import com.qlangtech.tis.plugin.ontology.OntologyProperty;
import com.qlangtech.tis.plugin.ontology.aggregation.AggregationCriteria;
import com.qlangtech.tis.plugin.ontology.aggregation.AggregationResult;
import com.qlangtech.tis.plugin.ontology.impl.objtype.ObjectSetRowQuery;
import com.qlangtech.tis.plugin.ontology.impl.objtype.ObjectTypeBinding;
import com.qlangtech.tis.plugin.ontology.workshop.enums.VariableType;
import com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopVariable;
import com.qlangtech.tis.plugin.ontology.workshop.model.definition.FunctionConfig;
import com.qlangtech.tis.plugin.ontology.workshop.model.definition.ObjectSetDefinitionConfig;
import com.qlangtech.tis.plugin.ontology.workshop.model.definition.VariableDefinitionConfig;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * {@link VariableType#OBJECT_SET} —— 一组 Ontology 对象，含其属性与关系。
 *
 * <p>本类<b>没有自己的字段</b>：变量类型由 Java 类型承担，其余配置全部继承自
 * {@link WorkshopVariable}。可用哪些定义方式见
 * {@code VariableDefinitionConfig#TYPE_DEFINITIONS} 中本类型对应的那一行
 * —— 不在这里重复，避免矩阵出现第二份会过期的副本。
 *
 * @see ObjectSetDefinitionConfig
 */
public class ObjectSetVariable extends WorkshopVariable {

    private static final Logger logger = LoggerFactory.getLogger(ObjectSetVariable.class);

    /**
     * 本对象集对应对象类型的主键属性名；没有可推导的主键（函数计算 / 外部写入定义、
     * 或对象类型显式关闭主键）时返回 null。
     *
     * <p>消费方（ObjectTableWidget#get_data）据此把主键值另存为
     * {@link ObjectTypeBinding#KEY_ROW_IDENTITY}，交互侧不必猜主键列叫什么。
     */
    public String getIdentityPropName() {
        if (this.definitionConfig instanceof ObjectSetDefinitionConfig objSetCfg) {
            OntologyObjectType objectType = objSetCfg.loadObjectTypeDetail();
            return objectType.hasDisablePK() ? null : objectType.getPk().map(OntologyProperty::getName).orElse(null);
        }
        return null;
    }

    public List<RowMap> executeQuery(ObjectTypeBinding.DataSourceQueryCriteria criteria) {
        applyConfiguredFilters(criteria);
        return resolveDataSourceBinding().executeQuery(criteria);
    }

    /**
     * 「一次性取前 N 行」的薄封装，供卡片 / 图表类 widget 的 {@code get_data} 使用。
     *
     * <p>与 {@link #executeQuery} 的唯一差异是「criteria 由本方法构造」：调用方只需要给出
     * 要取的列与行数上限，不必各自拼 {@code page} / {@code pageSize} / 主键列 —— 那三样
     * 每写错一处都是静默的（页码不对取到空表、漏主键则 {@code __rid} 全空、少列则图表缺维度）。
     *
     * <p><b>仍然经 {@link #executeQuery} 落地</b>，因此静态过滤照常施加。不要另起一条直接调
     * {@code resolveDataSourceBinding().executeQuery()} 的路径 —— 那会让「对象集变量上配的过滤条件」
     * 在这类 widget 上静默失效，而表格类 widget 却正常，形成难以排查的不一致。
     *
     * @param targetCols 本次要取的列；不能为空
     * @param maxRows    行数上限，内部按 {@link ObjectSetRowQuery#clampMaxRows} 夹取
     * @return 已补好 {@link ObjectTypeBinding#KEY_ROW_IDENTITY} 的行
     */
    public List<RowMap> fetchRows(List<String> targetCols, int maxRows) {
        // 主键名要取两次会各读一遍本体定义（磁盘 + 缓存），这里只解析一次
        final String identityProp = getIdentityPropName();
        ObjectTypeBinding.DataSourceQueryCriteria criteria
                = ObjectSetRowQuery.createCriteria(targetCols, identityProp, maxRows);
        // ⚠️ __rid 由这里补而不是由 executeQuery 补 —— 后者对交互侧的保留键一无所知
        return ObjectSetRowQuery.decorateIdentity(executeQuery(criteria), identityProp);
    }

    /**
     * 统计该对象集的总行数（服务端分页用）。
     *
     * <p>与 {@link #executeQuery} 走同一条链路，只是落在 {@link ObjectTypeBinding#countQuery} 上。
     */
    public long countQuery(ObjectTypeBinding.DataSourceQueryCriteria criteria) {
        applyConfiguredFilters(criteria);
        return resolveDataSourceBinding().countQuery(criteria);
    }

    /**
     * 按定义方式分派到绑定的数据源实现 —— {@link #executeQuery} / {@link #countQuery} 共用的那一段。
     *
     * <p>抽出来是为了让「哪些定义方式能取数」只有一个出处：两处各写一份的话，将来给
     * FunctionConfig 补上 schema 时极易只改一处，表现为「翻页能数出总数、却取不到行」这种
     * 一半好一半坏的状态。
     */
    private ObjectTypeBinding resolveDataSourceBinding() {
        if (this.definitionConfig instanceof ObjectSetDefinitionConfig objSetCfg) {
            OntologyObjectType objectType = objSetCfg.loadObjectTypeDetail();
            ObjectTypeBinding dataSourceBinding = objectType.getDataSourceBinding();
            if (!dataSourceBinding.hasBound()) {
                throw TisException.create("objectType:" + objectType.getName() + " has not bind dataSource");
            }
            return dataSourceBinding;

        } else if (this.definitionConfig instanceof FunctionConfig funcCfg) {
            // FunctionConfig 确实在 VariableDefinitionConfig.TYPE_DEFINITIONS 的 OBJECT_SET 行里（保持不动），
            // 但函数输出的 schema 目前没有被建模：FunctionConfig 只有自由文本 functionId 与 inputs，
            // FunctionImplementation.returnType 是标量白名单，没有任何本体对象类型的表达，
            // functionId 也未与 OntologyFunction 建立引用。因此这里取不到 List<OntologyProperty>，
            // 只能给出可执行的报错，引导用户改用对象集合定义，而不是返回一个静默的空列列表。
            throw new IllegalStateException("对象集合变量 '" + this.getName() + "' 由函数计算定义（functionId="
                    + funcCfg.functionId + "），函数输出的 schema 未建模，无法推导列定义。"
                    + "如需按对象属性自动填充列，请把该变量的定义方式改为 'ObjectSet Definition（对象集合定义）'。");
        } else {
            throw new IllegalStateException("illegal type:" + this.definitionConfig.getClass().getName()
                    + " support:" + VariableDefinitionConfig.definitionsOf(this.getVariableType()).stream().map(Class::getName).collect(Collectors.joining(",")));
        }
    }

    /**
     * 数据库端聚合：委托给已绑定的数据源。
     *
     * <p>与 {@link #getColsMeta()} 共用 {@link #resolveDataSourceBinding()}，
     * 因此 FunctionConfig 定义同样在这里给出可执行的报错，而不是静默返回空结果。
     */
    public AggregationResult executeAggregation(AggregationCriteria criteria) {
        applyConfiguredFilters(criteria);
        return resolveDataSourceBinding().executeAggregation(criteria);
    }

    /**
     * 把 {@link ObjectSetDefinitionConfig#filters} 里落盘的<b>静态</b>过滤条件施加到 criteria 上。
     *
     * <p>施加点放在本类的三个取数方法内部（{@link #executeQuery} / {@link #countQuery} /
     * {@link #executeAggregation}），而不是交给各个调用方 —— 这样「任何后端消费方都自动获得
     * 静态条件」只有一个出处。顺序上必须先施加、后解析数据源：{@link #resolveDataSourceBinding()}
     * 对未绑定数据源会抛，那条报错比「过滤条件配错」更该先出现。
     *
     * <p><b>幂等不在这里做</b>：{@code ObjectTableWidget#get_data} 用同一个 criteria 先
     * {@code countQuery} 再 {@code executeQuery}，本方法会被调用两次；重复的谓词由
     * {@link ObjectTypeBinding.DataSourceQueryCriteria#addFilter} 内建的去重丢掉。
     *
     * <p>定义方式不是 {@link ObjectSetDefinitionConfig} 时<b>静默 return</b>：{@code FunctionConfig}
     * 那条路要由 {@link #resolveDataSourceBinding()} 给出带指引的报错，在这里提前抛
     * ClassCastException 会把那句可执行的提示盖掉。
     */
    public void applyConfiguredFilters(ObjectTypeBinding.DataSourceQueryCriteria criteria) {
        if (!(this.definitionConfig instanceof ObjectSetDefinitionConfig objSetCfg)) {
            return;
        }
        List<ObjectTypeBinding.FilterCriterion> configured = objSetCfg.resolveFilterCriteria();
        if (configured.isEmpty()) {
            return;
        }
        Set<String> objectTypeCols = resolveObjectTypeColNames(objSetCfg);
        for (ObjectTypeBinding.FilterCriterion criterion : configured) {
            if (!objectTypeCols.contains(criterion.property())) {
                // 配置里的属性可能因本体改名而失效。跳过而不是抛：一个过期的静态条件不该让整张表报错
                logger.warn("object set variable:" + this.getName() + " filter property:'" + criterion.property()
                        + "' does not exist in objectType:" + objSetCfg.objectType + ", skipped");
                continue;
            }
            addFilter(criteria, criterion);
        }
    }

    /**
     * 把<b>中继</b>的运行时条件（{@code ObjectSetDefinitionConfig#filterVar} 指向的过滤变量的当前值，
     * 由前端随请求上送）施加到 criteria 上。
     *
     * <p>与 {@link #applyConfiguredFilters} 的差异只在「属性不存在时怎么办」：这里<b>抛</b>。
     * 属性名来自 HTTP 请求，静默跳过等于默默忽略错配（甚至恶意）的列名。多值算子
     * （{@code in_} / {@code inOrNull}）必须给集合，形态校验交给
     * {@link ObjectTypeBinding.DataSourceQueryCriteria#addFilter}。
     *
     * @param runtime 中继条件；空/null 时什么都不做
     */
    public void applyRuntimeFilters(ObjectTypeBinding.DataSourceQueryCriteria criteria,
                                    List<ObjectTypeBinding.FilterCriterion> runtime) {
        if (runtime == null || runtime.isEmpty()) {
            return;
        }
        if (!(this.definitionConfig instanceof ObjectSetDefinitionConfig objSetCfg)) {
            // 同 applyConfiguredFilters：非对象集合定义时不在这里抛，交给 resolveDataSourceBinding()
            // 给出带指引的报错，否则本方法的报错会把它盖掉。
            return;
        }
        Set<String> objectTypeCols = resolveObjectTypeColNames(objSetCfg);
        for (ObjectTypeBinding.FilterCriterion criterion : runtime) {
            String property = criterion == null ? null : StringUtils.trimToNull(criterion.property());
            if (property == null) {
                // 走这条链路的值全由本工程前端生成，出现空白就是 bug；静默跳过等于悄悄放宽过滤
                throw new IllegalArgumentException("runtime filter property can not be blank");
            }
            if (!objectTypeCols.contains(property)) {
                throw new IllegalArgumentException("runtime filter property:'" + property
                        + "' does not exist in objectType:" + objSetCfg.objectType + ", cols:" + objectTypeCols);
            }
            addFilter(criteria, new ObjectTypeBinding.FilterCriterion(property, criterion.operator(), criterion.value()));
        }
    }

    /**
     * 静态 / 中继两条路径共用的施加动作。
     *
     * <p>⚠️ 先 {@code addFilterCol} 再 {@code addFilter}：{@code addFilter} 的白名单只认
     * 「调用方自己声明过的列」，而过滤属性通常<b>不在</b>表格展示列里。为什么不直接把过滤列
     * 塞进 {@code targetCols}：那会让它进 SELECT，{@code DataSourceBinding#readRow} 按
     * targetCols 取值，于是每行都多带一个前端没申请过的键。
     *
     * <p>算子已经是枚举（{@code FilterOperator}），合法性由类型保证；{@code addFilter} 只再做
     * 「多值算子必须给集合」这一项形态校验，不在这里重复。
     */
    private static void addFilter(ObjectTypeBinding.DataSourceQueryCriteria criteria,
                                  ObjectTypeBinding.FilterCriterion criterion) {
        criteria.addFilterCol(criterion.property());
        criteria.addFilter(criterion.property(), criterion.operator(), criterion.value());
    }

    /**
     * 当前对象类型的属性名集合 —— 这一层才是过滤列真正的白名单。
     *
     * <p>不能省：{@code addFilterCol} 只是「调用方声明过的列」，若把请求里的 property 直接转手
     * 传给它，白名单就形同虚设（等于把防注入边界交给前端）。与
     * {@code AggregationPushdownPlanner#requireCol} 是同一个做法。
     */
    private static Set<String> resolveObjectTypeColNames(ObjectSetDefinitionConfig objSetCfg) {
        List<OntologyProperty> cols = objSetCfg.getColsMeta();
        Set<String> names = new HashSet<>();
        for (OntologyProperty col : cols) {
            names.add(col.getName());
        }
        return names;
    }

    public List<OntologyProperty> getColsMeta() {

        if (this.definitionConfig instanceof ObjectSetDefinitionConfig objSetCfg) {
            return objSetCfg.getColsMeta();
        } else if (this.definitionConfig instanceof FunctionConfig funcCfg) {
            // FunctionConfig 确实在 VariableDefinitionConfig.TYPE_DEFINITIONS 的 OBJECT_SET 行里（保持不动），
            // 但函数输出的 schema 目前没有被建模：FunctionConfig 只有自由文本 functionId 与 inputs，
            // FunctionImplementation.returnType 是标量白名单，没有任何本体对象类型的表达，
            // functionId 也未与 OntologyFunction 建立引用。因此这里取不到 List<OntologyProperty>，
            // 只能给出可执行的报错，引导用户改用对象集合定义，而不是返回一个静默的空列列表。
            throw new IllegalStateException("对象集合变量 '" + this.getName() + "' 由函数计算定义（functionId="
                    + funcCfg.functionId + "），函数输出的 schema 未建模，无法推导列定义。"
                    + "如需按对象属性自动填充列，请把该变量的定义方式改为 'ObjectSet Definition（对象集合定义）'。");
        } else {
            throw new IllegalStateException("illegal type:" + this.definitionConfig.getClass().getName()
                    + " support:" + VariableDefinitionConfig.definitionsOf(this.getVariableType()).stream().map(Class::getName).collect(Collectors.joining(",")));
        }
    }

    @Override
    public VariableType getVariableType() {
        return VariableType.OBJECT_SET;
    }

    @TISExtension
    public static class DefaultDescriptor extends BasicDescriptor {

        public DefaultDescriptor() {
            super(VariableType.OBJECT_SET);
        }
    }
}
