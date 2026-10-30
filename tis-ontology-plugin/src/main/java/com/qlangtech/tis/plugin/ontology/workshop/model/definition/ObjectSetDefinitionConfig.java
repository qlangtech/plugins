package com.qlangtech.tis.plugin.ontology.workshop.model.definition;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.Ontology;
import com.qlangtech.tis.plugin.ontology.OntologyObjectType;
import com.qlangtech.tis.plugin.ontology.OntologyProperty;
import com.qlangtech.tis.plugin.ontology.impl.OntologyPluginMeta;
import com.qlangtech.tis.plugin.ontology.impl.objtype.ObjectTypeBinding;
import com.qlangtech.tis.plugin.ontology.workshop.model.definition.ObjectSetFilterConfig.FilterOperator;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.WidgetOptionHelper;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 对象集合定义配置
 * 变量值通过 Ontology 查询获取的对象集合
 */
public class ObjectSetDefinitionConfig extends VariableDefinitionConfig {

    private static final Logger logger = LoggerFactory.getLogger(ObjectSetDefinitionConfig.class);

    public static final String KEY_OBJECT_TYPE = "objectType";
    public static final String KEY_FILTERS = "filters";
    public static final String KEY_FILTER_VAR = "filterVar";
    /**
     * 过滤条件列表里的属性字段。级联落点写成 {@code "filters[].property"}（见
     * {@code DefaultDescriptor} 构造器）—— {@code []} 是 valueChangePipe 的「作用于列表每一行」
     * 约定标记，只在宿主这一侧解析，元素类 {@link ObjectSetFilterConfig} 不认识它。
     */
    public static final String KEY_FILTER_PROPERTY = "property";

    @FormField(ordinal = 0, type = FormFieldType.ENUM, validate = {Validator.require})
    public String objectType;

    /**
     * 静态过滤条件：多行之间是「与」关系，整体为空表示不限制。
     *
     * <p>本字段曾是 {@code String}（TEXTAREA，让构建者手写
     * {@code [{"property":...,"operator":...,"value":...}]}）。结构化改造后，操作符有了下拉、
     * 属性候选跟随 {@link #objectType} 联动，见 {@link ObjectSetFilterConfig}。
     */
    @FormField(ordinal = 1, desClazz = ObjectSetFilterConfig.class, type = FormFieldType.MULTI_DESCRIBLE_PLUGIN)
    public List<ObjectSetFilterConfig> filters = new ArrayList<>();

    public OntologyObjectType loadObjectTypeDetail() {
        OntologyPluginMeta pluginMeta = OntologyPluginMeta.createPluginMeta();
        OntologyObjectType objectType = OntologyObjectType.loadObjectTypeDetail(pluginMeta.getDomain(), this.objectType);
        return objectType;
    }

    public List<OntologyProperty> getColsMeta() {
        return loadObjectTypeDetail().getCols();
    }

    /**
     * 把落盘的静态过滤条件归一成 {@link ObjectTypeBinding.FilterCriterion} 列表，供后端取数路径
     * （{@code ObjectSetVariable#executeQuery} / {@code countQuery} / {@code executeAggregation}）
     * 施加。以前这份 {@link #filters} <b>只在浏览器里</b>被消费（前端 {@code variable.service.ts}
     * 归一后随 REST 请求上送），后端取数一律无视它 —— 表现为「配了过滤条件，表格照样是全量」。
     *
     * <h3>归一规则</h3>
     * 与前端 {@code object-set-filter.model.ts#toOntologyFilters} 逐条对齐（该函数还额外负责
     * 拆 {@code {impl, vals}} 信封，后端拿到的已经是摊平对象）。两边必须得到<b>相同</b>的谓词：
     * 同一份配置走 REST（前端归一）与走 {@code get_data}（本方法归一）若归一出不同结果，
     * 就会变成「同一个对象集，两条路径查出不同行数」这种最难排查的偏差。
     *
     * <h3>容错口径</h3>
     * 一律「跳过该行 + warn」，<b>不抛</b>：一个配错的条件不该让整张表打不开。三条跳过规则：
     * <ul>
     *   <li>{@code property} 为空 —— 同 {@code WorkshopOntologyService} 的口径；</li>
     *   <li>{@code value} 为 null / 空白 —— 表单里 {@code value} 没有 {@code require}，
     *       「没填」与「用户故意要 {@code = ''}」在输入端无从区分，而放行会让
     *       {@code appendWhere} 绑定一个空值参数；</li>
     *   <li>多值算子按逗号拆完为空（如 {@code " , "}）—— {@code appendWhere} 对空 {@code IN}
     *       集合会抛，且 {@code IN ('')} 永远查不中。</li>
     * </ul>
     *
     * <p>本方法<b>不</b>校验属性是否真的存在于对象类型上（那是
     * {@code ObjectSetVariable#applyConfiguredFilters} 的职责，它才有本体上下文）：
     * 保持纯函数才能在单测里直接构造配置对象验证归一结果。
     */
    public List<ObjectTypeBinding.FilterCriterion> resolveFilterCriteria() {
        // 字段初值 new ArrayList<>() 只在构造器里生效，而本类实例是从 XML 反序列化出来的
        // （XStream 不走构造器）；存量配置里没有 filters 节点时这里就是 null。
        if (this.filters == null || this.filters.isEmpty()) {
            return Collections.emptyList();
        }
        List<ObjectTypeBinding.FilterCriterion> criteria = new ArrayList<>();
        for (ObjectSetFilterConfig filter : this.filters) {
            if (filter == null) {
                continue;
            }
            String property = StringUtils.trimToNull(filter.property);
            if (property == null) {
                logger.warn("objectSet definition filters has a row with blank property, skipped, label:"
                        + filter.label);
                continue;
            }
            // operator 同样可能是 null（XStream 不走字段初值），回落到表单里的默认算子
            FilterOperator operator = filter.operator == null ? FilterOperator.equals : filter.operator;
            Object value = normalizeFilterValue(operator, filter.value);
            if (value == null) {
                logger.warn("objectSet definition filter has blank value, skipped, label:" + filter.label
                        + ", property:" + property + ", operator:" + operator.valueToken());
                continue;
            }
            criteria.add(new ObjectTypeBinding.FilterCriterion(property, operator.valueToken(), value));
        }
        return criteria;
    }

    /**
     * 比较值归一，与前端 {@code normalizeFilterValue} 逐条对齐：多值算子按逗号拆分并去空白，
     * 拆完为空即视为「没有值」（返回 null 由调用方跳过）。
     *
     * <p>非多值算子原样透传、<b>不 trim</b>：{@code contains} 的 {@code %v%} 由
     * {@code DataSourceBinding#appendWhere} 自己拼，提前 trim 会改变用户填写的语义
     * （比如刻意包含首尾空格）。
     */
    private static Object normalizeFilterValue(FilterOperator operator, String value) {
        if (StringUtils.isBlank(value)) {
            return null;
        }
        if (!operator.isMultiValued()) {
            return value;
        }
        List<String> parts = new ArrayList<>();
        for (String part : StringUtils.split(value, ',')) {
            String trimmed = StringUtils.trimToNull(part);
            if (trimmed != null) {
                parts.add(trimmed);
            }
        }
        return parts.isEmpty() ? null : parts;
    }

    /**
     * 本对象集合在查询时额外叠加哪个「对象集合过滤器」变量（{@code VariableType.OBJECT_SET_FILTER}）的谓词。
     *
     * <h3>为什么需要这个字段</h3>
     * 本字段出现之前，对象集变量是<b>自足</b>的：它只读自己的 {@link #filters}。而
     * {@code FilterListWidget.filterOutputVar}、{@code ChartLayer.selectionAsFilterVar} 这类
     * Widget 写出的过滤条件，在本字段之前<b>没有任何读取方</b>——写进变量值缓存就再没人看，
     * 整条链路是断的（参见设计文档 05-08 中把 {@code selectionAsFilter} 标为 P2 占位）。
     * 本字段把「对象集」与「过滤器」两种变量接起来：对象集变量解析时读取被引用过滤变量的值，
     * 与自身的 {@link #filters} 合并后一起发给 Ontology 查询。
     *
     * <h3>为什么是「引用变量」而不是「让 Widget 直连对象集」</h3>
     * 过滤条件由谁产生（FilterList、图层刷选）与对象集查什么，是两件独立的事。经变量中转后
     * 一个过滤变量可以同时供多个对象集/表格消费，且过滤变量的值变化会沿变量依赖图自动触发
     * 本变量重算——前端 {@code VariableService} 已按变量依赖做拓扑排序重算，无需本字段为此做什么。
     *
     * <h3>合并语义</h3>
     * 与 {@link #filters} 是<b>「与」</b>关系（两组谓词拼接成一个列表）；被引用的过滤变量若
     * 声明的 objectType 与本配置的 {@link #objectType} 不一致，其谓词会被丢弃并记日志，
     * 而不是静默施加一个查不出任何数据的条件。值形态见前端
     * {@code src/workshop/models/object-set-filter.model.ts} 的 {@code ObjectSetFilterValue}——
     * 该值只活在浏览器内存中、不落盘，故 Java 侧没有对应类。
     *
     * <h3>可选性</h3>
     * 不配置时行为与加此字段之前完全一致（只用 {@link #filters}），存量配置不受影响。
     */
    @FormField(ordinal = 2,type = FormFieldType.SELECTABLE)
    public String filterVar;

    /**
     * 通过 Ontology 查询服务取对象集合，前端直接发起
     */
    @Override
    public boolean isBackendComputed() {
        return false;
    }

    @TISExtension
    public static class DefaultDescriptor extends BasicDescriptor implements DescriptorUseableShortComment {

        public DefaultDescriptor() {
            super();
            // 只列 OBJECT_SET_FILTER 类型的变量：本字段要读的是「过滤条件」，不是别的值。
            // registerSelectOptions 收的是 Callable，惰性求值——无插件上下文的 descriptor 构造
            // （如单测里直接 new DefaultDescriptor()）不会被触发而抛异常。
            this.registerSelectOptions(KEY_FILTER_VAR, WidgetOptionHelper::getObjectSetFilterVariableOptions);

            // 对象类型 → 过滤行里每一条的「属性」候选。
            //
            // 落点写 "filters[].property" 而不是 "filters.property"：filters 是
            // MULTI_DESCRIBLE_PLUGIN 列表字段，级联要作用到列表里的<b>每一条记录</b>，
            // "[]" 是该写法的约定标记（前端 item-prop-val.component.ts#propValChange 解析）。
            // 后端只把这个字符串原样存下、原样回传，从不解析它。
            //
            // fromField 必须是同 descriptor 的普通非 Describable 字段（RootFormProperties
            // 对 Describable 字段根本不查 pipe），objectType 满足；落点下钻进子表单则不受此限。
            this.valueChangePipe(KEY_OBJECT_TYPE, KEY_FILTERS + "[]." + KEY_FILTER_PROPERTY)
                    .render((pluginMeta, param) -> {
                        // 1. 没选对象类型：属性候选只能是空。注意不能直接 return null ——
                        //    PluginAction 用 Collectors.toMap 汇总，null value 会 NPE。
                        // 2. 对象类型取不到（本体域被删、类型改名等）：降级为空列表而不是抛异常。
                        //    本方法在表单渲染期被调用，一个配错的类型名不该让整张表单打不开；
                        //    与 LinkReference.DftDesc 的宽 catch 是同一取舍。
                        try {
                            String objectType = StringUtils.trimToNull(param.getString(KEY_OBJECT_TYPE));
                            if (objectType == null) {
                                return Collections.emptyList();
                            }
                            OntologyPluginMeta meta = OntologyPluginMeta.createPluginMeta(pluginMeta);
                            OntologyObjectType objType = Ontology.loadObjectTypeDetail(meta.getDomain(), objectType);
                            if (objType == null) {
                                return Collections.emptyList();
                            }
                            return objType.getColOpts();
                        } catch (Exception e) {
                            logger.warn("load object type relevant cols failed, filter property options is empty", e);
                            return Collections.emptyList();
                        }
                    });
        }

        @Override
        public String getDisplayName() {
            return "ObjectSet Definition";
        }

        @Override
        public String shortComment() {
            return "对象集合定义";
        }
    }
}