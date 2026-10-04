package com.qlangtech.tis.plugin.ontology.workshop.model.header;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;

import java.io.Serializable;

/**
 * Workshop Header 的排布方向 —— 水平顶栏 / 垂直侧栏二选一。
 *
 * <p>为什么是两个子类、而不是「一个 {@code HeaderOrientation} 枚举 + 两个可空载荷字段」：
 * 后者允许出现「垂直方向 + 高度 160px」这类<b>自相矛盾</b>的状态（高度是水平方向专属、
 * 宽度才是垂直方向专属），编译器无从约束。改造前正是这个形态。现在每种方向各自的
 * 载荷字段只出现在自己的子类上：{@link HorizontalOrientation#height} /
 * {@link VerticalOrientation#width}，矛盾态在类型层面即不可表示。
 *
 * <p>同一取舍见同包下的 {@code widget.SizingMode}、
 * {@code model.overlay.OverlayTypeConfig}、{@code model.definition.VariableDefinitionConfig}。
 *
 * <p>本类<b>刻意不声明任何 {@code @FormField} 字段</b>：字段全在子类里。原因是 ordinal
 * 在同一次 {@code PropertyType.buildPropertyTypes} 里排序，基类与子类各声明一份会撞号
 * （两个子类的字段 ordinal 都从 0 起算，基类再占 0 就重复了）。也因此本类不需要自己的
 * {@code .json}。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/20
 */
public abstract class HeaderOrientation implements Describable<HeaderOrientation>, Serializable {

    private static final long serialVersionUID = 1L;

    public abstract static class BasicDescriptor extends Descriptor<HeaderOrientation> {
    }
}
