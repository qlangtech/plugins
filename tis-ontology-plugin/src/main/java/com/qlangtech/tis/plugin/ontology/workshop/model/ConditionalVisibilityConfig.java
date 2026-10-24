package com.qlangtech.tis.plugin.ontology.workshop.model;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;

import java.io.Serializable;

/**
 * 条件可见性配置的抽象基类（支持多态）。
 *
 * <p>宿主（Section / Widget 显示配置 / 按钮）里的 {@code conditionalVisibility} 字段原先声明为
 * 具体类 {@link ConditionalVisibility}，「不启用」只能靠留空（{@code null}）表达 —— 表单里
 * 无法显式选择「关闭」，类型层面也不存在「关闭」状态。现在改为抽象 Describable：
 * <ul>
 *   <li>{@link ConditionalVisibilityOff} —— 零字段，表示不启用；</li>
 *   <li>{@link ConditionalVisibility} —— 默认实现，引用变量 + 变量为假时的呈现方式。</li>
 * </ul>
 *
 * <p>本类<b>刻意不声明任何 {@code @FormField} 字段</b>：字段全在子类里。ordinal 在同一次
 * {@code PropertyType.buildPropertyTypes} 里排序，基类与子类各声明一份会撞号。
 * 也因此本类不需要自己的 {@code .json}。
 *
 * <p>同一取舍见 {@code model/section/HeaderFormat}、{@code model/section/DropHandling}。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/22
 */
public abstract class ConditionalVisibilityConfig implements Describable<ConditionalVisibilityConfig>, Serializable {

    private static final long serialVersionUID = 1L;

    public abstract static class BasicDescriptor extends Descriptor<ConditionalVisibilityConfig> {
    }
}
