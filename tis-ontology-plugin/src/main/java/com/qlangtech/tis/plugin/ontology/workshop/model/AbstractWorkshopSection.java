package com.qlangtech.tis.plugin.ontology.workshop.model;

import com.qlangtech.tis.plugin.IdentityName;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.workshop.model.section.SectionLayout;
import com.qlangtech.tis.plugin.ontology.workshop.model.section.SectionStyleConfig;
import com.qlangtech.tis.plugin.ontology.workshop.widget.WorkshopWidget;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Workshop Section 的抽象基类，承载 {@link WorkshopSection} 与 {@link SectionChild} 共享的表单字段。
 * <p>
 * 设计要点：
 * <ol>
 *   <li><b>本类不实现 {@code Describable}</b>：{@code Describable<Self>} 由两个子类各自声明
 *       （{@code WorkshopSection implements Describable<WorkshopSection>}、
 *       {@code SectionChild implements Describable<SectionChild>}）。这样既能避开递归泛型
 *       {@code AbstractWorkshopSection<T extends AbstractWorkshopSection<T>>}（TIS 全库无此先例），
 *       又能让 {@code SectionChild} 以自身为 self-type 满足
 *       {@code MultiDescribleElementDescriptor<T extends Describable<T>>} 的约束。
 *       {@code @FormField} 字段按祖先链反射收集（{@code PluginExtraProps.visitAncestorsClass}，
 *       走裸 {@code getSuperclass()} 链），不依赖本类是否 {@code Describable}。</li>
 *   <li><b>identity 上收到本基类</b>：{@link #name} 标 {@code identity=true}，本类 {@code implements IdentityName}。
 *       两个子类都继承 identity。这意味着 {@link WorkshopSection} 从「刻意无 identity」变为「有 identity」——
 *       无害：section 不落盘、无 store 消费者，identity 只在框架双向校验（恰好 1 个）与表单里作行键存在。</li>
 *   <li>ordinal 沿用 0–5，子类新增字段从 6 起，避免撞号。</li>
 * </ol>
 *
 * @see WorkshopSection
 * @see SectionChild
 */
public abstract class AbstractWorkshopSection implements Serializable, IdentityName {

    private static final long serialVersionUID = 1L;

    @FormField(identity = true, ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String name;

    /**
     * 区块布局。<b>布局身份由取值自身的 Java 类型承担</b> ——
     * {@code ColumnsLayout} / {@code RowsLayout} / {@code TabsLayout} /
     * {@code FlowLayout} / {@code ToolbarLayout} / {@code LoopLayout}。
     *
     * <p>本字段原先是一对强相关的字段：一个 {@code SectionLayout} <b>枚举</b>说"是哪种布局"，
     * 外加一个 {@code SectionLayoutConfig} 抽象类说"那种布局的参数"。两者现已合并 ——
     * 枚举与抽象类并存的形态允许自相矛盾的状态（{@code layout = COLUMNS} 配一个
     * {@code TabsLayout}），且两套分类体系已经漂移（枚举 6 个常量 / 配置类 3 个子类）。
     */
    @FormField(ordinal = 1, validate = {Validator.require})
    public SectionLayout layout;

    @FormField(ordinal = 2)
    public SectionStyleConfig styleConfig;

    /** 条件可见性，与 Widget 的 {@code WidgetDisplayConfig.conditionalVisibility} 共用同一类型 */
    @FormField(ordinal = 3)
    public ConditionalVisibility conditionalVisibility;



    @FormField(ordinal = 5, type = FormFieldType.INT_NUMBER)
    public Integer sortOrder = 0;

    /**
     * 区块内的 Widget 列表。元素类型是 tis-plugin 的 Widget 基类 —— 具体是哪种 Widget
     * 由元素自身的 Java 类型承担（实例 JSON 上表现为扁平 {@code impl} 键）。
     * <p>
     * transient、不进表单：widget 仍由画布投放区添加。
     */
    public transient List<WorkshopWidget> widgets = new ArrayList<>();

    public void addWidget(WorkshopWidget widget) {
        if (widgets == null) {
            widgets = new ArrayList<>();
        }
        widgets.add(widget);
    }

    @Override
    public String identityValue() {
        return this.name;
    }
}
