package com.qlangtech.tis.plugin.ontology.workshop.widget;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ds.IMultiElement;

import java.io.Serializable;

/**
 * 过滤列表（{@code FilterListWidget}）中<b>单个过滤项</b>的结构化配置。
 *
 * <h3>为什么是一个独立类型</h3>
 * 与 {@link WidgetColumnConfig} 同一个理由：过滤项是「组件类型 + 属性 + 显示名」的三元结构，
 * 让构建者手写 JSON 数组字符串（{@code [{"component":"keyword",...}]}）既没有表单校验，
 * 也没有下拉可选项。声明成 {@code Describable} 后由 TIS 的元组行编辑器渲染。
 *
 * <h3>为什么没有「候选项列表」字段</h3>
 * {@code single_select} / {@code multi_select} 的候选项<b>不落盘</b>，而是在运行时由
 * 「本 Widget 选中的对象集变量 → 其 {@code ObjectSetDefinitionConfig.objectType} →
 * 该 {@code property} 上声明的枚举约束」推导出来。理由：
 * <ul>
 *   <li>落盘一份候选项副本，就必然出现「本体改了、过滤项没改」的不一致，且无人能发现</li>
 *   <li>同一批候选项会被多个过滤项、多个 Widget 重复配置</li>
 *   <li>嵌套的第二层列表（{@code List<OptionConfig>}）需要再实现一套元组行编辑器</li>
 * </ul>
 *
 * <h3>为什么没有验证「property 确实是该对象类型上的属性」</h3>
 * 本类<b>看不到</b>它所属 Widget 的 {@code objectSetVar}（表单是逐字段独立的，
 * 元组行的校验上下文里没有兄弟字段的值），因此无法在此处做级联校验。
 * 该约束目前只能靠运行时兜底：查不到属性的一方（Ontology 查询）返回空结果。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/16
 * @see FilterItemElementCreatorFactory
 */
public class FilterItemConfig implements Describable<FilterItemConfig>, IMultiElement, Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 过滤作用的 Ontology 属性名（{@code objectType} 上的 property key）。
     *
     * <p>{@code identity = true}：TIS 把同一多选列表内 identity 字段相同的两行判为重复，
     * 与 {@code WidgetColumnConfig.prop} 的口径一致。同一个属性在列表里出现两次是没有意义的
     * ——两条谓词会以「与」的关系施加到同一个属性上（区间场景需要它，但区间由图表刷选承担，
     * 不由本 Widget 承担）。
     */
    @FormField(identity = true, ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
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
     */
    @FormField(ordinal = 2, type = FormFieldType.ENUM, validate = {Validator.require})
    public FilterComponent component;

    /**
     * 谓词算子。<b>可选</b>：留空时按 {@link #component} 推导
     * （关键词→contains、单选→equals、多选→in、日期→equals）。
     *
     * <p>之所以留着这个覆盖口，是因为「控件形态」与「比较语义」不是一一对应的：
     * 同一个日期选择器，既可以表达「等于某天」，也可以表达「不晚于某天」。
     * 不给覆盖口就只能改控件形态来表达语义，那是把两件事混成一件。
     */
    @FormField(ordinal = 3, type = FormFieldType.ENUM, advance = true)
    public FilterOperator operator;

    /**
     * 作为元组行的标识。
     *
     * <p>{@link IMultiElement#getName()} 被 {@code FormFieldType.SelectedItem} 用作
     * 选项的 name 与 value，取 {@link #property} 而非 {@link #label}：
     * 标签是展示用的、允许重复，属性名才是这一行的身份。
     */
    @Override
    public String getName() {
        return this.property;
    }

    /**
     * 过滤控件的形态。
     *
     * <p><b>常量名即落盘值</b>：{@code PropertyType.createEnumOptions} 取 {@code e.name()}
     * 作为选项的 val（{@code resolveEnumLabel} 的反射逻辑已被注释，label 也是常量名）。
     * 因此这里的常量名是<b>线格式</b>，前端 {@code filter-list.component.ts} 的模板
     * {@code *ngIf} 直接按这些字面量比较 —— 改成 {@code single-select} 这种带连字符的写法
     * 在 Java 里不可能（连字符不是合法标识符），所以是前端跟着后端走，不是反过来。
     *
     * <p>中文标签由 {@code FilterItemConfig.json} 的 {@code enum} 数组提供，
     * 不经由本枚举的 {@link DescriptorUseableShortComment}（那条路当前不通）。
     */
    public enum FilterComponent implements DescriptorUseableShortComment {
        keyword("关键词"),
        single_select("单选"),
        multi_select("多选"),
        date_picker("日期选择");

        private final String label;

        FilterComponent(String label) {
            this.label = label;
        }

        @Override
        public String shortComment() {
            return this.label;
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
    public static class DftDescriptor extends Descriptor<FilterItemConfig> {
    }
}
