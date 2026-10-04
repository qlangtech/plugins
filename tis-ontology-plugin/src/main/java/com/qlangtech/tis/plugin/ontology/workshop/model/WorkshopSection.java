package com.qlangtech.tis.plugin.ontology.workshop.model;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.workshop.enums.SectionLayout;
import com.qlangtech.tis.plugin.ontology.workshop.model.section.DropHandling;
import com.qlangtech.tis.plugin.ontology.workshop.model.section.SectionLayoutConfig;
import com.qlangtech.tis.plugin.ontology.workshop.model.section.SectionStyleConfig;
import com.qlangtech.tis.plugin.ontology.workshop.widget.WorkshopWidget;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Workshop Section 实体
 * <p>
 * 本类<b>不</b>实现 {@code IdentityName}、也<b>不</b>给 {@link #name} 加 identity 语义 ——
 * section 既没有独立 store 也不落盘（它是页面前端状态的一部分），没有需要 identity 的消费者。
 * 框架对 identity 字段数量做双向校验（实现了 IdentityName 就必须恰好 1 个，不实现就必须 0 个），
 * 所以「不实现」与「不加」必须成对出现。
 * <p>
 * 原先还有一个私有的 {@code id}（构造器生成 UUID、无 {@code @FormField}、非 identity），
 * 全工程零调用点，已删除。注意<b>前端</b>的 section 模型另有自己的 id 且是活的
 * （{@code section.parentId}、CDK drop list id、树节点 key 都在用），与本类无关。
 */
public final class WorkshopSection implements Describable<WorkshopSection>, Serializable {

    private static final long serialVersionUID = 1L;

    @FormField(ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String name;

    @FormField(ordinal = 1, type = FormFieldType.ENUM, validate = {Validator.require})
    public SectionLayout layout;

    @FormField(ordinal = 2)
    public SectionLayoutConfig layoutConfig;

    @FormField(ordinal = 3)
    public SectionStyleConfig styleConfig;

    /** 条件可见性，与 Widget 的 {@code WidgetDisplayConfig.conditionalVisibility} 共用同一类型 */
    @FormField(ordinal = 4)
    public ConditionalVisibility conditionalVisibility;

    @FormField(ordinal = 5)
    public DropHandling dropHandling;

    @FormField(ordinal = 6, type = FormFieldType.INT_NUMBER)
    public Integer sortOrder = 0;

    /**
     * 区块内的 Widget 列表。元素类型是 tis-plugin 的 Widget 基类 —— 具体是哪种 Widget
     * 由元素自身的 Java 类型承担（实例 JSON 上表现为扁平 {@code impl} 键）。
     */
    // @FormField(ordinal = 10, type = FormFieldType.MULTI_SELECTABLE)
    public transient List<WorkshopWidget> widgets = new ArrayList<>();

    // @FormField(ordinal = 11, type = FormFieldType.MULTI_SELECTABLE)
    public transient List<WorkshopSection> childSections = new ArrayList<>();

    public void addWidget(WorkshopWidget widget) {
        if (widgets == null) {
            widgets = new ArrayList<>();
        }
        widgets.add(widget);
    }

    @TISExtension
    public static class DefaultDescriptor extends Descriptor<WorkshopSection> {
        @Override
        public String getDisplayName() {
            return "Workshop Section";
        }
    }
}

