package com.qlangtech.tis.plugin.ontology.workshop.model;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;

/**
 * 按类别值分桶（Categorical Bucketing）
 *
 * <p>适用于 X 轴属性为<b>字符串或布尔</b>的图层。这类属性没有「粒度」可言，能配的只有
 * 「取哪些值」，因此本子类只多出一个 {@link #mode}。
 *
 * <p>两种取值方式是<b>速度与精度的取舍</b>，不是「粗略 / 精确」的同义重复：
 * <ul>
 *   <li>{@link CategoricalMode#TOP_VALUES} —— 只扫前 1000 个不同的值，响应快，
 *       适合基数小、或只需要看头部几类的场景（如告警类型、机型）</li>
 *   <li>{@link CategoricalMode#EXACT_VALUES} —— 精确扫描，最多考虑 10000 个值，
 *       响应更慢，适合基数大且尾部也有意义的场景（如机场代码）</li>
 * </ul>
 * 两种方式的 {@link XAxisBucketing#limit} 天花板不同，故本类的默认值下调为
 * {@link #TOP_VALUES_LIMIT}，见构造函数。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/15
 * @see XAxisBucketing
 */
public class CategoricalBucketing extends XAxisBucketing {

    private static final long serialVersionUID = 1L;

    /**
     * 「前 N 个高频值」这条路能考虑的值个数上限 —— 对象集聚合 API 的既定天花板。
     */
    public static final int TOP_VALUES_LIMIT = 1000;

    /**
     * 取值方式；默认「前 N 个高频值」。
     */
    @FormField(ordinal = 10, type = FormFieldType.ENUM, validate = {Validator.require})
    public CategoricalMode mode = CategoricalMode.TOP_VALUES;

    public CategoricalBucketing() {
        // 默认值随取值方式走：基类默认的 10000 是 hard cap，但对 topValues 这条路是够不着的
        // 天花板。用构造函数而非 json 的 dftVal 覆盖 —— 后者只影响表单回显，落盘的实例
        // 仍会保留基类字段初始化器的值，两条路会分叉。
        this.limit = TOP_VALUES_LIMIT;
    }

    /**
     * 取值方式。
     *
     * <p>{@link #shortComment()} 只作为选项的说明文案下发；选项显示 label 来自
     * {@code CategoricalBucketing.json} 的 {@code enum} 数组。
     */
    public enum CategoricalMode implements DescriptorUseableShortComment {
        TOP_VALUES("前 N 个高频值 —— 扫前 1000 个不同的值，响应快；适合基数小的属性"),
        EXACT_VALUES("精确去重值 —— 最多考虑 10000 个值，响应较慢；适合基数大的属性");

        private final String comment;

        CategoricalMode(String comment) {
            this.comment = comment;
        }

        @Override
        public String shortComment() {
            return comment;
        }
    }

    @TISExtension
    public static class DefaultDescriptor extends BasicDescriptor implements DescriptorUseableShortComment {

        @Override
        public String getDisplayName() {
            return "Categorical Bucketing";
        }

        @Override
        public String shortComment() {
            return "按类别值分桶";
        }

        // validateLimit（桶数上限）由 BasicDescriptor 提供，此处无需重声明 —— 见那里的说明。
    }
}
