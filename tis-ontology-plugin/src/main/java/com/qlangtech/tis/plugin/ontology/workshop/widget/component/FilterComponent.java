package com.qlangtech.tis.plugin.ontology.workshop.widget.component;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;

import java.io.Serializable;

/**
 * 过滤列表（{@code FilterListWidget}）里一行过滤项<b>用哪种控件</b> ——
 * {@link KeywordComponent} / {@link DatePickerComponent} / {@link SingleSelectComponent} /
 * {@link MultiSelectComponent} 四选一。
 *
 * <h3>为什么从枚举改成多态插件</h3>
 * 原先 {@code FilterItemConfig.component} 是一个 {@code FilterComponent} 枚举，四种取值之间
 * <b>载荷不同</b>却共用同一张扁平表单：只有单选 / 多选需要「候选项从哪来」，而枚举表达不出
 * 「这个字段只属于这两个取值」。改成多态之后，{@code optionsSource} 只声明在
 * {@link AbstractSelectComponent} 上，关键词与日期选择器天然没有这个字段 —— 编译器与表单
 * 同时被约束住，不会出现「选了关键词却还配着一份候选项」的自相矛盾状态。
 *
 * <h3>为什么本类零字段</h3>
 * 四个子类的形态差异全在「有没有 optionsSource」这一处，由
 * {@link AbstractSelectComponent} 承载。本类没有自己的 {@code @FormField}，因此也
 * <b>不需要</b> {@code FilterComponent.json}（见 {@code TestWorkshopFormDefaults} 的
 * 「零字段类不要多写 json」口径）。
 *
 * <h3>为什么 {@link BasicDescriptor} 不挂 {@code @TISExtension}</h3>
 * sezpoz 索引不过滤抽象类，给抽象基类的 descriptor 挂 {@code @TISExtension} 会让抽象类型
 * 混进「控件形态」选择器。同理，本类的 {@code BasicDescriptor} 与
 * {@link AbstractOpsSource.BasicDescriptor} <b>必须各自独立</b>：共用一个
 * {@code Descriptor<FilterComponent>} 会让「选项来源」的两个实现混进控件形态下拉
 * （{@code DescriptorExtensionList} 按 {@code getT()} 的赋值关系筛选，两类会互相命中）。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/11/2
 * @see FilterItemConfig 的 component 字段
 */
public abstract class FilterComponent implements Describable<FilterComponent>, Serializable {

    private static final long serialVersionUID = 1L;

    public abstract static class BasicDescriptor extends Descriptor<FilterComponent> {
    }
}
