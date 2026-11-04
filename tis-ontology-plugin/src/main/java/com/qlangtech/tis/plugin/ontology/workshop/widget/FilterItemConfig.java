package com.qlangtech.tis.plugin.ontology.workshop.widget;

import com.google.common.collect.Lists;
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.IPluginStore;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.workshop.widget.component.AbstractSelectComponent;
import com.qlangtech.tis.plugin.ontology.workshop.widget.component.FilterComponent;
import com.qlangtech.tis.plugin.ontology.workshop.widget.component.KeywordComponent;
import com.qlangtech.tis.plugin.ontology.workshop.widget.component.ObjectSetOpsSource;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

/**
 * 过滤列表（{@code FilterListWidget}）中<b>单个过滤项</b>的结构化配置。
 *
 * <h3>为什么是一个独立类型</h3>
 * 与 {@link WidgetColumnConfig} 同一个理由：过滤项是「组件形态 + 目标属性 + 显示名」的三元结构，
 * 让构建者手写 JSON 数组字符串（{@code [{"component":"keyword",...}]}）既没有表单校验，
 * 也没有下拉可选项。声明成 {@code Describable} 并由宿主的
 * {@code MULTI_DESCRIBLE_PLUGIN} + {@code desClazz} 驱动子表单渲染，即无需自定义行编辑器。
 *
 * <h3>{@link #component} 为什么是抽象插件而不是枚举</h3>
 * 枚举只能表达「长什么样」，表达不了「单选/多选的可选项从哪来」。候选项来源本身是一个
 * 有字段的配置（手填列表 / 取自某个对象集变量并指定 label、val 属性），必须承载为嵌套子表单：
 * <ul>
 *   <li>{@link FilterComponent} —— 四种形态（keyword / date-picker / single-select / multi-select）</li>
 *   <li>{@link AbstractSelectComponent} —— 单选/多选的公共父类，声明 {@code optionsSource}</li>
 *   <li>{@code AbstractOpsSource} —— 候选项来源（manual / object-set）</li>
 * </ul>
 *
 * <h3>为什么没有验证「property 确实是该对象类型上的属性」</h3>
 * 本类<b>看不到</b>它所属 Widget 的 {@code objectSetVar}（子表单行的校验上下文里没有兄弟字段的
 * 值）：候选列表由宿主 Descriptor 的 {@code valueChangePipe} 在渲染期回填，只影响可选项、
 * 不做落盘校验。该约束目前只能靠运行时兜底：查不到属性的一方（Ontology 查询）返回空结果。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/16
 */
public class FilterItemConfig implements Describable<FilterItemConfig>, IPluginStore.MultiDescribleElement, Serializable {

    private static final long serialVersionUID = 1L;

    public static final String KEY_PROPERTY = "property";
    public static final String KEY_LABEL = "label";
    public static final String KEY_COMPONENT = "component";
    public static final String KEY_OPERATOR = "operator";

    /**
     * 过滤作用的 Ontology 属性名（{@code objectType} 上的 property key），同时是本行的记录键。
     *
     * <p>{@code identity = true}：子表单以该字段的值作行的键，同值行会被<b>静默合并</b>
     * （前端 {@code descVal.vals[pk.primary] = item}），而子表单框架不会再报「名称重复」。
     * 这与 {@code WidgetColumnConfig.prop} 的口径一致：同一个属性在列表里出现两次没有意义
     * ——两条谓词会以「与」的关系施加到同一个属性上（区间场景由图表刷选承担，
     * 不由本 Widget 承担）。
     *
     * <p>候选列表由宿主 {@code FilterListWidget} 的级联管道供给：
     * {@code valueChangePipe("objectSetVar", "filters[].property")}，即「对象集变量变了，
     * 逐行刷新属性候选」。本类自己看不到宿主的 {@code objectSetVar}，所以这里必须声明
     * {@code SELECTABLE} 而不是 {@code ENUM}：SELECTABLE 的候选本来就是运行时供给的，
     * ENUM 则意味着候选固定。
     */
    @FormField(identity = true, ordinal = 0, type = FormFieldType.SELECTABLE, validate = {Validator.require})
    public String property;

    /**
     * 过滤控件在页面上的显示名（如「出发地」）。
     */
    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String label;

    /**
     * 过滤控件的形态。决定页面渲染成输入框、单选、多选还是日期选择器，
     * 也决定未显式指定 {@link #operator} 时默认用哪个算子（见
     * {@code filter-list.component.ts} 的算子推导）。
     *
     * <p>字段类型声明为抽象的 {@link FilterComponent}，<b>不</b>写 {@code type}、<b>不</b>写
     * {@code desClazz}：{@code PropertyType.isDescribable()} 为真时表单把它渲染成嵌套的多态子表单，
     * 可选项来自 {@code TIS.get().getDescriptorList(FilterComponent.class)}。写成
     * {@code type = ENUM} 会让子表单机制退化成单值下拉，选「单选」时也就没有地方挂
     * {@code optionsSource}。
     *
     * <p>Java 初值 {@link KeywordComponent} 与 {@code FilterItemConfig.json} 里的
     * {@code "dftVal": "keyword"} 是<b>一对</b>：前端新建表单的初值只认 {@code eprops.dftVal}，
     * Java 初始化器它一概不读；而 {@code TestWorkshopFormDefaults} 又要求两者逐字一致。
     */
    @FormField(ordinal = 2, validate = {Validator.require})
    public FilterComponent component = new KeywordComponent();

    /**
     * 谓词算子。<b>可选</b>：留空时按 {@link #component} 推导
     * （关键词→contains、单选→equals、多选→in、日期→equals）。
     *
     * <p>之所以留着这个覆盖口，是因为「控件形态」与「比较语义」不是一一对应的：
     * 同一个日期选择器，既可以表达「等于某天」，也可以表达「不晚于某天」。
     * 不给覆盖口就只能改控件形态来表达语义，那是把两件事混成一件。
     */
    @FormField(ordinal = 3, type = FormFieldType.ENUM)
    public FilterOperator operator;

    /**
     * 子表单行的标识：取 {@link #property} 而非 {@link #label}，
     * 标签是展示用的、允许重复，属性名才是这一行的身份。
     */
    @Override
    public String identityValue() {
        return this.property;
    }

    /**
     * XStream 反序列化不跑构造器、也不跑字段初始化器：存量配置（{@code component} 还是枚举标量
     * 的那批）反序列化时，抽象类型接不住 {@code <component>single_select</component>}，
     * {@code RobustReflectionConverter} 会把它<b>静默丢弃</b>，这里兜底成默认形态。
     *
     * <p>⚠️ 原文已不可恢复 —— 落盘里只剩形如 {@code <component class="...KeywordComponent"/>}
     * 的节点才会被读回，标量节点读不回。不要假装这是无损迁移。
     *
     * <p>必须声明在<b>本类</b>：{@code readResolve} 只对自己的实例生效，父类的
     * {@code readResolve} 不会沿继承链下发到子类实例。
     */
    private Object readResolve() {
        this.normalize();
        return this;
    }

    /**
     * 把缺失的多态字段补成默认值；幂等。
     */
    public void normalize() {
        if (this.component == null) {
            this.component = new KeywordComponent();
        }
        if (this.component instanceof AbstractSelectComponent selectComponent
                && selectComponent.optionsSource == null) {
            selectComponent.optionsSource = new ObjectSetOpsSource();
        }
    }

    /**
     * 谓词算子。取值与前端 {@code OntologyFilterOperator} 联合类型一一对应。
     *
     * <p>后端无需认识这些取值：{@code WorkshopOntologyService.Filter.operator} 声明为普通
     * {@code String}，{@code WorkshopOntologyAction.doQueryObjectSet} 只做 {@code getString}
     * 透传，没有白名单校验。本枚举存在的意义是让构建者有个可选项列表，而不是做协议约束。
     *
     * <h3>为什么叫 {@code in_} 而不是 {@code in}</h3>
     * {@code in} 是 Java 关键字，不能做标识符。同一文件里的 {@link WidgetColumnConfig.ColumnFormat}
     * 对 {@code boolean} 用了同样的尾下划线写法（{@code boolean_}），此处沿用。
     * 落盘值是 {@code in_}，前端在 {@code filter-list.component.ts} 的算子归一里
     * 把它翻译回线格式的 {@code in} —— 下游 {@code ObjectSetFilterValue} 只认 {@code in}。
     */
    public enum FilterOperator implements DescriptorUseableShortComment {
        equals("等于"),
        contains("包含"),
        greaterThan("大于"),
        lessThan("小于"),
        greaterThanOrEqual("大于等于"),
        lessThanOrEqual("小于等于"),
        in_("属于集合");

        private final String label;

        FilterOperator(String label) {
            this.label = label;
        }

        @Override
        public String shortComment() {
            return this.label;
        }
    }

    @TISExtension
    public static class DftDescriptor extends IPluginStore.MultiDescribleElementDescriptor<FilterItemConfig> {

        public DftDescriptor() {
            super();
            // SELECTABLE 字段必须在 descriptor 上登记候选来源，否则 DescriptorsJSON#getSelectOptions
            // 直接抛异常、整张表单打不开。这里登记一个空列表作兜底：真实的候选由宿主
            // FilterListWidget 的级联管道在渲染期回填（见 FilterItemConfig#property）。
            this.registerSelectOptions(KEY_PROPERTY, Collections::emptyList);
        }

        @Override
        public String getDisplayName() {
            return "Filter Item";
        }

        @Override
        public String shortComment() {
            return "过滤项";
        }

        @Override
        public ViewStyle viewStyle() {
            return ViewStyle.Table;
        }

        @Override
        public List<ColConfig> colsConfig() {
            // Table 视图要求至少一列 clickable（getExtractProps 会校验），否则用户无法打开行编辑面板。
            //
            // ⚠️ KEY_COMPONENT 不列出来：component 现在是多态描述符对象，
            // MultiChildViewDescriptor.cellText 对对象值恒渲染成空串 —— 列出来就是一条恒空白列，
            // 比没有这一列更让人困惑。用户点开行编辑面板即可看到形态。
            return Lists.newArrayList(//
                    new ColConfig(KEY_PROPERTY, 25).setClickable()//
                    , new ColConfig(KEY_LABEL, 25)//
                    , new ColConfig(KEY_OPERATOR));
        }

        @Override
        public int maxHeight() {
            return 300;
        }
    }
}
