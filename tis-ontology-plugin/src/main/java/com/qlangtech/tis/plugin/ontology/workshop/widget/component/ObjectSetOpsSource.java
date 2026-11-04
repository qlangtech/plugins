package com.qlangtech.tis.plugin.ontology.workshop.widget.component;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.ibatis.RowMap;
import com.qlangtech.tis.manage.common.Option;
import com.qlangtech.tis.plugin.IdentityName;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.OntologyProperty;
import com.qlangtech.tis.plugin.ontology.aggregation.AggregationCriteria;
import com.qlangtech.tis.plugin.ontology.aggregation.AggregationResult;
import com.qlangtech.tis.plugin.ontology.aggregation.GroupKey;
import com.qlangtech.tis.plugin.ontology.impl.OntologyPluginMeta;
import com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopVariable;
import com.qlangtech.tis.plugin.ontology.workshop.model.variable.ObjectSetVariable;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.WidgetOptionHelper;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * 候选项来自一个预先定义的 <b>ObjectSetVariable</b>（一般是维度表对应的那个），
 * 并指定该变量所指对象类型上<b>哪个属性作 label、哪个属性作 val</b>。
 *
 * <p>落盘的<b>不是</b>一份候选项副本，而是「怎么取」：module GET 序列化时由
 * {@link #createOptions()} 按本类三个字段去目标库做一次 distinct 查询
 * （{@code AggregationCriteria} 的 GROUP BY 下推），结果随 {@code appendVals.options} 下发。
 * 理由：维度表随时会变，落盘副本必然出现「本体改了、过滤项没改」的不一致，且无人能发现。
 *
 * <p>⚠️ 因为候选是<b>读时现算</b>的，编辑本类三个字段后要等到下一次 module GET（刷新页面）
 * 才会看到新的候选 —— 保存只落盘、不回读 module。
 *
 * <h3>label / value 属性为什么都不是必填</h3>
 * 它们由 {@link #sourceObjectSetVar} 级联供给候选，在用户还没选变量时必然为空。
 * 挂 {@code Validator.require} 会让「刚切到本类型、还没来得及选」这一步直接卡住保存。
 * 运行期由 {@link #createOptions()} 兜底（value 取不到就退回用 {@code sourceObjectSetVar}
 * 对应对象类型的主键、label 退回 value，都取不到则返回空列表并记一条 warn）。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/11/2
 * @see ManualOpsSource
 * @see AbstractOpsSource#createOptions()
 */
public class ObjectSetOpsSource extends AbstractOpsSource {

    private static final long serialVersionUID = 1L;

    private static final Logger logger = LoggerFactory.getLogger(ObjectSetOpsSource.class);

    /**
     * 单个过滤项最多回多少条候选。
     *
     * <p>它同时是 SQL 的 {@code LIMIT} 与回传体量边界。候选项要渲染成一列单选/多选，
     * 上千条本来就是配置错误；超过这个数时只取前 500 条并记一条 warn（不让页面白等）。
     */
    public static final int MAX_OPTIONS = 500;

    public static final String KEY_SOURCE_OBJECT_SET_VAR = "sourceObjectSetVar";
    public static final String KEY_LABEL_PROPERTY = "labelProperty";
    public static final String KEY_VALUE_PROPERTY = "valueProperty";

    /**
     * 候选项的取值来源：一个「对象集合定义」型的变量。
     *
     * <p>候选由 {@code WidgetOptionHelper#getObjectSetSourceVariableOptions()} 供给 ——
     * 只列出<b>可查询</b>的对象集定义变量（外部写入型对象集是选中行载体，取数会抛
     * illegal type，不该出现在这里）。
     */
    @FormField(ordinal = 0, type = FormFieldType.SELECTABLE, validate = {Validator.require})
    public String sourceObjectSetVar;

    /**
     * 作显示文本的属性名。
     *
     * <p>候选由本 descriptor 声明的级联管道供给：
     * {@code valueChangePipe("sourceObjectSetVar", "labelProperty", "valueProperty")} ——
     * 变量一变，两个属性下拉同时刷新。
     */
    @FormField(ordinal = 1, type = FormFieldType.SELECTABLE, validate = {})
    public String labelProperty;

    /**
     * 作选项值（即最终写进谓词的值）的属性名。
     */
    @FormField(ordinal = 2, type = FormFieldType.SELECTABLE, validate = {})
    public String valueProperty;

    /**
     * 取自对象集：对变量所指对象类型做一次按属性去重的查询。
     *
     * <p>label / value 两个属性都允许留空，兜底口径写在字段注释里：
     * value 留空退回对象类型主键，label 留空退回 value。
     *
     * <p><b>整个方法体包 try/catch</b>：这里会触达插件存储（{@code WorkshopVariable.loadDetail}）
     * 与数据库（{@code executeAggregation}），失败形态包括变量已被删除、方言不支持聚合、
     * 变量定义方式不可取数（{@code FunctionConfig}）、属性名在本体重命名后失效等。
     * 候选项只是「页面上的可选项」，取不到的最坏后果是没有选项，而不是整个 module GET 失败 ——
     * 所以一律降级成空列表 + warn（契约见 {@link AbstractOpsSource#createOptions()}）。
     */
    @Override
    protected List<Option> createOptions() {
        try {
            return resolveOptions();
        } catch (Exception e) {
            logger.warn("[filter-list] 对象集变量 '" + sourceObjectSetVar + "' 的候选项解析失败，本次按空列表处理", e);
            return Collections.emptyList();
        }
    }

    private List<Option> resolveOptions() {
        List<Option> options = new ArrayList<>();
        if (StringUtils.isBlank(sourceObjectSetVar)) {
            // 用户在表单里刚切到本类型、还没来得及选变量，是合法中间态，不必 warn
            return options;
        }
        WorkshopVariable variable = WorkshopVariable.loadDetail(sourceObjectSetVar);
        if (!(variable instanceof ObjectSetVariable objSetVar)) {
            logger.warn("[filter-list] 候选项来源 '{}' 不是对象集变量（实际 {}），按空列表处理",
                    sourceObjectSetVar,
                    variable == null ? "null" : variable.getClass().getSimpleName());
            return options;
        }

        // 属性白名单来自对象类型本身，绝不把请求里的字段名直接交给 SQL 拼装
        Set<String> cols = new LinkedHashSet<>();
        List<OntologyProperty> colsMeta = objSetVar.getColsMeta();
        for (OntologyProperty col : colsMeta) {
            cols.add(col.getName());
        }

        String valueProp = StringUtils.trimToNull(valueProperty);
        if (valueProp == null) {
            valueProp = objSetVar.getIdentityPropName();
        }
        String labelProp = StringUtils.trimToNull(labelProperty);
        if (labelProp == null) {
            labelProp = valueProp;
        }
        if (labelProp == null || valueProp == null) {
            logger.warn("[filter-list] 对象集变量 '{}' 取不到主键，且未指定选项值属性，无法生成候选项",
                    sourceObjectSetVar);
            return options;
        }
        if (!cols.contains(labelProp) || !cols.contains(valueProp)) {
            logger.warn("[filter-list] 对象集变量 '{}' 上没有属性 labelProperty='{}' / valueProperty='{}'"
                            + "（现有属性 {}），按空列表处理 —— 通常是本体重命名后没有回来改这里",
                    sourceObjectSetVar, labelProp, valueProp, cols);
            return options;
        }

        AggregationCriteria criteria = new AggregationCriteria();
        criteria.addTargetCol(labelProp);
        criteria.addGroupKey(GroupKey.rawColumn(labelProp));
        boolean sameProp = labelProp.equals(valueProp);
        if (!sameProp) {
            criteria.addTargetCol(valueProp);
            criteria.addGroupKey(GroupKey.rawColumn(valueProp));
        }
        criteria.setMaxGroups(MAX_OPTIONS);

        AggregationResult aggregation = objSetVar.executeAggregation(criteria);
        for (RowMap row : aggregation.rows()) {
            Object labelVal = row.get(AggregationCriteria.groupAlias(0));
            Object valueVal = sameProp ? labelVal : row.get(AggregationCriteria.groupAlias(1));
            if (valueVal == null) {
                // 分组键 c1 没有 IS NOT NULL 约束（方言侧只对第一个分组键加），这里自行跳过
                continue;
            }
            options.add(new Option(labelVal == null ? String.valueOf(valueVal) : String.valueOf(labelVal)
                    , String.valueOf(valueVal)));
        }
        if (aggregation.truncated()) {
            logger.warn("[filter-list] 对象集变量 '{}' 的去重结果超过 {} 条，候选项已截断",
                    sourceObjectSetVar, MAX_OPTIONS);
        }
        return options;
    }


    @TISExtension
    public static class DefaultDescriptor extends BasicDescriptor implements DescriptorUseableShortComment {

        public DefaultDescriptor() {
            super();
            // SELECTABLE 字段未注册候选来源会让 DescriptorsJSON#getSelectOptions 抛
            // 「has not been register」，整张表单打不开。三个字段一个都不能漏。
            this.registerSelectOptions(KEY_SOURCE_OBJECT_SET_VAR,
                    WidgetOptionHelper::getObjectSetSourceVariableOptions);
            // 两个属性字段的真实候选由下面的级联管道在渲染期逐实例回填，这里登记空列表兜底
            this.registerSelectOptions(KEY_LABEL_PROPERTY, Collections::emptyList);
            this.registerSelectOptions(KEY_VALUE_PROPERTY, Collections::emptyList);

            // === 级联管道：变量变了，两个属性候选一起刷新 ===
            // ⚠️ toField 必须按字典序声明（labelProperty < valueProperty）：
            // Descriptor#getValueChangeFromFieldKeys 用 Arrays.binarySearch 查 toField 数组，
            // 无序时找不到 → 级联声明看上去还在，但「主字段是否被自校验白名单排除」那条判断
            // 会静默失效（这个来源字段本身不该再被校验为必填之外的形态）。
            this.valueChangePipe(KEY_SOURCE_OBJECT_SET_VAR, KEY_LABEL_PROPERTY, KEY_VALUE_PROPERTY)
                    .render((pluginMeta, param) -> {
                        String selectedVar = param.getString(KEY_SOURCE_OBJECT_SET_VAR);
                        if (selectedVar == null) {
                            // render 的返回值不能为 null（调用方 Collectors.toMap 会 NPE）
                            return Collections.<IdentityName>emptyList();
                        }
                        return WidgetOptionHelper.getObjectPropertyOptions(
                                selectedVar, OntologyPluginMeta.createPluginMeta(pluginMeta));
                    });
        }

        /**
         * 线格式：前端派生 kebab token {@code object-set}。
         */
        @Override
        public String getDisplayName() {
            return "object-set";
        }

        @Override
        public String shortComment() {
            return "取自对象集：按维度表去重";
        }
    }
}
