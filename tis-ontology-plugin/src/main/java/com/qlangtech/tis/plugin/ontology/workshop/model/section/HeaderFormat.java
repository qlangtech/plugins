package com.qlangtech.tis.plugin.ontology.workshop.model.section;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;

import java.io.Serializable;

/**
 * Section 标题格式（抽象基类，支持多态）。
 *
 * <p>原先是一个 {@code HeaderFormat} <b>枚举</b>（NONE / TITLE / TITLE_WITH_DESCRIPTION），
 * 包含一个「标题和描述」的常量却没有任何地方能输入描述 —— 「描述在哪里填」这一问题在枚举
 * 下无解。现在改为抽象 Describable 子类：描述字段只存在于
 * {@link TitleWithDescriptionHeaderFormat} 子类上，矛盾态（没有描述却选了标题和描述）
 * 在类型层面不可表示。
 *
 * <p>同一取舍见 {@code DropHandling}、{@code HeaderOrientation} 等。
 *
 * <p>本类<b>刻意不声明任何 {@code @FormField} 字段</b>：字段全在子类里。原因是 ordinal
 * 在同一次 {@code PropertyType.buildPropertyTypes} 里排序，基类与子类各声明一份会撞号。
 * 也因此本类不需要自己的 {@code .json}。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/22
 */
public abstract class HeaderFormat implements Describable<HeaderFormat>, Serializable {

    private static final long serialVersionUID = 1L;

    public abstract static class BasicDescriptor extends Descriptor<HeaderFormat> {
    }
}