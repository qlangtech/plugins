package com.qlangtech.tis.plugin.ontology.workshop.model.section;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;

import java.io.Serializable;

/**
 * Workshop Section 布局（抽象基类，支持多态）。
 *
 * <p>本类由原先的 {@code SectionLayoutConfig} 更名而来，同时吸收了原先旁挂在
 * {@code WorkshopSection} 上的 {@code SectionLayout} <b>枚举</b>（COLUMNS / ROWS /
 * TABS / FLOW / TOOLBAR / LOOP）。合并的原因见
 * {@code model/header/HeaderOrientation} 的类注释：一个扁平类配一个判别枚举、
 * 再加一个可为 null 的载荷字段，允许出现自相矛盾的状态 ——
 * {@code layout = COLUMNS} 配一个 {@code TabsLayout} 能编译、能过校验，
 * 编译器无从约束。而且两套分类体系已经漂移：枚举有 6 个常量，
 * 配置子类却只有 3 个。现在布局身份完全由子类 Java 类型承担，两套体系不再可能不一致。
 *
 * <p><b>本类刻意不声明任何 {@code @FormField} 字段</b>：字段全在子类里。原因是 ordinal
 * 在同一次 {@code PropertyType.buildPropertyTypes} 里排序，基类与子类各声明一份会撞号
 * （六个子类的字段 ordinal 都从 0 起算，基类再占 0 就重复了）。也因此本类不需要自己的
 * {@code .json}。
 *
 * <p><b>子类命名约定：必须以 {@code Layout} 结尾。</b> 前端
 * （{@code tis-console/src/workshop/models/click-action.model.ts} 的
 * {@code resolveImplKind}）以剥后缀 + PascalCase 转 kebab-case 的方式从 FQCN 推导短 key，
 * 故 {@code ColumnsLayout} → {@code columns}、{@code RowsLayout} → {@code rows}……
 * 与 {@code section.model.ts} 里既有的 {@code SectionLayout} 小写联合类型逐字一致。
 * 改类名会让前端那六个渲染分支全部失配 —— 且是静默的（只有 {@code *ngIf} 不命中，
 * 分区内容整块渲染成空白，不报任何错）。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @see com.qlangtech.tis.plugin.ontology.workshop.model.header.HeaderOrientation
 */
public abstract class SectionLayout implements Describable<SectionLayout>, Serializable {

    private static final long serialVersionUID = 1L;

    public abstract static class BasicDescriptor extends Descriptor<SectionLayout> {
    }
}
