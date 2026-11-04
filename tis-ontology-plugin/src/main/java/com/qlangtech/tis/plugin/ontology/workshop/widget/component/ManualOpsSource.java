package com.qlangtech.tis.plugin.ontology.workshop.widget.component;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.manage.common.Option;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.ontology.impl.action.widget.WidgetOptionConfig;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * 手填候选项：由构建者在一张 val/label 子表格里逐条录入。
 *
 * <p>复用 {@link WidgetOptionConfig}，不新造元素类：它已经满足
 * {@code MULTI_DESCRIBLE_PLUGIN} 的全部契约（实现 {@code MultiDescribleElement}、
 * 恰好一个 {@code identity = true} 的 {@code val}、descriptor extends
 * {@code MultiDescribleElementDescriptor}）。同一形状的列表在本工程已有多处使用者
 * （{@code EnumWidget.options} / {@code SelectableWidget.options}），再造一份只会多一处漂移点。
 *
 * <p>⚠️ {@code val} 是子表单的行键（{@code identity = true}）：同一份列表里两条
 * {@code val} 相同的记录会被前端<b>静默合并成一行</b>（{@code descVal.vals[pk.primary] = item}），
 * 框架不会报「值重复」。
 *
 * <p>不需要 {@code valueChangePipe}：与 {@link ObjectSetOpsSource} 不同，本类的候选项
 * 完全来自落盘配置，与任何其他字段无关。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/11/2
 */
public class ManualOpsSource extends AbstractOpsSource {

    private static final long serialVersionUID = 1L;

    public static final String KEY_OPTIONS = "options";

    /**
     * 候选项清单。
     *
     * <p>非必填：空列表 = 还没配，前端渲染成没有可选项的单选 / 多选控件（与「配置缺失」
     * 视觉上一致，但不应该在保存时把它当成错误 —— 用户很可能先选控件形态再回来填候选项）。
     *
     * <p>{@code desClazz} 指向元素类而不是靠泛型推断：{@code PropertyType} 用
     * {@code fieldListElementClazz} 决定选择器与元素契约校验，漏写会让整个字段渲染成一个
     * 无元素的空列表（编译期不报错）。
     */
    @FormField(ordinal = 0, type = FormFieldType.MULTI_DESCRIBLE_PLUGIN, desClazz = WidgetOptionConfig.class,
            validate = {})
    public List<WidgetOptionConfig> options;

    /**
     * 手填候选项：直接映射，按 {@code val} 去重。
     *
     * <p>去重的必要性：前端子表单以 {@code val} 作行键，同值的两行在<b>渲染期</b>就已经被
     * 静默合并；但如果存量数据绕过了表单（例如直接改 XML），落盘的仍可能是重复项。
     * 这里再挡一次，保证线上的候选列表里不会出现两个等值选项（选哪个都一样，用户只会困惑）。
     *
     * <p>{@code label} 留空回落 {@code val}：「有值没字」的选项在页面上会渲染成一条空白项。
     *
     * <p>纯内存映射、没有可抛点，故不像 {@link ObjectSetOpsSource#createOptions()} 那样
     * 整体包 try/catch —— 只做空值兜底。
     */
    @Override
    protected List<Option> createOptions() {
        if (options == null || options.isEmpty()) {
            return Collections.emptyList();
        }
        List<Option> result = new ArrayList<>();
        Set<String> seen = new LinkedHashSet<>();
        for (WidgetOptionConfig option : options) {
            if (option == null || StringUtils.isBlank(option.val)) {
                continue;
            }
            if (!seen.add(option.val)) {
                continue;
            }
            result.add(new Option(StringUtils.defaultIfBlank(option.label, option.val), option.val));
        }
        return result;
    }

    @TISExtension
    public static class DefaultDescriptor extends BasicDescriptor implements DescriptorUseableShortComment {

        /**
         * 线格式：前端派生 kebab token {@code manual}。
         */
        @Override
        public String getDisplayName() {
            return "manual";
        }

        @Override
        public String shortComment() {
            return "手动录入：逐条填候选值";
        }
    }
}
