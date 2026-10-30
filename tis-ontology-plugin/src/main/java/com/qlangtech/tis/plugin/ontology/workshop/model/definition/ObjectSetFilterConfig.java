package com.qlangtech.tis.plugin.ontology.workshop.model.definition;

import com.google.common.collect.Lists;
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.manage.common.IEnumTypeSer;
import com.qlangtech.tis.plugin.IPluginStore;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.impl.objtype.ObjectTypeBinding;

import java.util.Collections;
import java.util.List;

/**
 * 对象集合定义（{@link ObjectSetDefinitionConfig}）中的<b>单行过滤条件</b>，供
 * {@code MULTI_DESCRIBLE_PLUGIN} 子表单渲染。
 *
 * <p>替代原先 {@code filters} 字段上让用户手写 JSON 数组字符串的做法（违反工程 CLAUDE.md
 * 原则一「禁止 JSON 字符串输入」）：手工写 JSON 既没有表单校验，也没法把「可选的操作符」和
 * 「该对象类型下真实存在的属性」这两份事实下发给构建者，只能靠记忆与试错。
 *
 * <h3>为什么 identity 是 {@link #label} 而不是 {@link #property}</h3>
 * 前端以 identity 字段的<b>值</b>作 {@code descVal.vals} 的 key（tis-console 的
 * {@code tis.plugin.ts} 里 {@code descVal.vals[pk.primary] = item}），同值即<b>静默覆盖</b>。
 * 区间查询（{@code amount >= 10} 与 {@code amount <= 100}）天然会对同一个属性产出两行，
 * 拿 property 当 identity 会让其中一行凭空消失。
 *
 * <p>刻意不加 {@code Validator.identity}：该校验器只允许 {@code [A-Z\da-z_\-]+}，
 * 会拒绝「金额下限」这种天然是中文的条件名。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/29
 * @see ObjectSetDefinitionConfig
 */
public class ObjectSetFilterConfig implements Describable<ObjectSetFilterConfig>,
        IPluginStore.MultiDescribleElement {

    public static final String KEY_LABEL = "label";
    public static final String KEY_PROPERTY = "property";
    public static final String KEY_OPERATOR = "operator";
    public static final String KEY_VALUE = "value";

    /**
     * 条件名，同时是本行的记录键（identity）。
     *
     * <p>纯展示 + 标识用，不参与查询；理由是同一个属性可能出现多行（见类注释）。
     */
    @FormField(identity = true, ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String label;

    /**
     * 过滤作用的 Ontology 属性名（对象类型上的 property key）。
     *
     * <p>候选列表由宿主 {@code ObjectSetDefinitionConfig} 的级联管道供给：
     * {@code valueChangePipe("objectType", "filters[].property")}，即「对象类型变了，
     * 逐行刷新属性候选」。本类自己<b>看不到</b>宿主的 {@code objectType}（元组行的校验上下文
     * 里没有兄弟字段的值），所以这里必须声明 {@code SELECTABLE} 而不是 {@code ENUM}：
     * SELECTABLE 的候选本来就是运行时供给的，ENUM 则意味着候选固定。
     */
    @FormField(ordinal = 1, type = FormFieldType.SELECTABLE, validate = {Validator.require})
    public String property;

    /**
     * 谓词算子。取值见 {@link FilterOperator}，其落盘 token 与
     * {@link ObjectTypeBinding.DataSourceQueryCriteria} 白名单逐字一致。
     */
    @FormField(ordinal = 2, type = FormFieldType.ENUM, validate = {Validator.require})
    public FilterOperator operator = FilterOperator.equals;

    /**
     * 比较值。
     *
     * <p>刻意用 {@code INPUTTEXT} 而不是给每种属性类型换控件：属性的 {@code OntologyType}
     * 只有在本类被渲染之后才知道（见 {@link #property} 的说明），而一行一种控件需要按行
     * 切换控件类型，当前子表单引擎不支持。{@link FilterOperator#in_} 时填逗号分隔的多个值。
     */
    @FormField(ordinal = 3, type = FormFieldType.INPUTTEXT)
    public String value;

    @Override
    public String identityValue() {
        return this.label;
    }

    /**
     * 谓词算子。
     *
     * <p><b>实现 {@link IEnumTypeSer} 的唯一目的是把 {@code in_} 落成 {@code in}。</b>
     * {@code in} 是 Java 关键字、不能作枚举常量名，而下游
     * {@code ObjectTypeBinding.DataSourceQueryCriteria#addFilter} 的操作符白名单里是字面量
     * {@code "in"}。若不实现本接口，选项 val 就是常量名 {@code in_}，会在查询期被白名单
     * 拒掉（{@code illegal filter operator}）。其余常量名与白名单字面量逐字相同，直接回退到
     * {@link Enum#name()}。
     *
     * <p>中文标签经 {@link DescriptorUseableShortComment#shortComment()} 作为选项的 help 下发，
     * 故 {@code ObjectSetFilterConfig.json} 里不声明 {@code enum} 数组（声明了会连带丢掉
     * 端类型/说明的自动派生）。
     *
     * @see ObjectTypeBinding.DataSourceQueryCriteria
     */
    public enum FilterOperator implements DescriptorUseableShortComment, IEnumTypeSer {
        equals(ObjectTypeBinding.DataSourceQueryCriteria.OP_EQUALS, "等于"),
        contains(ObjectTypeBinding.DataSourceQueryCriteria.OP_CONTAINS, "包含"),
        greaterThan(ObjectTypeBinding.DataSourceQueryCriteria.OP_GREATER_THAN, "大于"),
        lessThan(ObjectTypeBinding.DataSourceQueryCriteria.OP_LESS_THAN, "小于"),
        greaterThanOrEqual(ObjectTypeBinding.DataSourceQueryCriteria.OP_GREATER_THAN_OR_EQUAL, "大于等于"),
        lessThanOrEqual(ObjectTypeBinding.DataSourceQueryCriteria.OP_LESS_THAN_OR_EQUAL, "小于等于"),
        in_(ObjectTypeBinding.DataSourceQueryCriteria.OP_IN, "属于集合");

        private final String comment;
        private final String operatorToken;

        FilterOperator(String operatorToken, String comment) {
            this.comment = comment;
            this.operatorToken = operatorToken;
        }

        @Override
        public String shortComment() {
            return this.comment;
        }

        @Override
        public String valueToken() {
            // return this == in_ ? ObjectTypeBinding.DataSourceQueryCriteria.OP_IN : name();
            return operatorToken;
        }

        /**
         * 该算子的取值是否必须是多个值（{@code IN (...)}）。
         */
        public boolean isMultiValued() {
            return this == in_;
        }
    }

    @TISExtension
    public static class DefaultDescriptor extends IPluginStore.MultiDescribleElementDescriptor<ObjectSetFilterConfig> {

        public DefaultDescriptor() {
            super();
            // SELECTABLE 字段必须在 descriptor 上登记候选来源，否则 DescriptorsJSON#getSelectOptions
            // 直接抛异常、整张表单打不开。这里登记一个空列表作兜底：真实的候选由宿主
            // ObjectSetDefinitionConfig 的级联管道在渲染期回填（见 ObjectSetFilterConfig#property）。
            this.registerSelectOptions(KEY_PROPERTY, Collections::emptyList);
        }

        @Override
        public String getDisplayName() {
            return "Object Set Filter";
        }

        @Override
        public String shortComment() {
            return "对象集合过滤条件";
        }

        @Override
        public ViewStyle viewStyle() {
            return ViewStyle.Table;
        }

        @Override
        public List<ColConfig> colsConfig() {
            // Table 视图要求至少一列 clickable（getExtractProps 会校验），否则用户无法打开行编辑面板。
            return Lists.newArrayList(//
                    new ColConfig(KEY_LABEL, 25).setClickable()//
                    , new ColConfig(KEY_PROPERTY, 25)//
                    , new ColConfig(KEY_OPERATOR, 20)//
                    , new ColConfig(KEY_VALUE));
        }

        @Override
        public int maxHeight() {
            return 300;
        }
    }
}
