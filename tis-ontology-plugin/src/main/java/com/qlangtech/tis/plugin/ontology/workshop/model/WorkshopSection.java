package com.qlangtech.tis.plugin.ontology.workshop.model;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.workshop.enums.SectionLayout;
import com.qlangtech.tis.plugin.ontology.workshop.model.ConditionalVisibility;
import com.qlangtech.tis.plugin.ontology.workshop.model.section.DropHandling;
import com.qlangtech.tis.plugin.ontology.workshop.model.section.SectionLayoutConfig;
import com.qlangtech.tis.plugin.ontology.workshop.model.section.SectionStyleConfig;
import com.qlangtech.tis.plugin.ontology.workshop.widget.WorkshopWidget;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Workshop Section 实体
 */
public final class WorkshopSection implements Describable<WorkshopSection>, Serializable {

    private static final long serialVersionUID = 1L;

    @FormField(ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String name;

    @FormField(ordinal = 1, type = FormFieldType.SELECTABLE, validate = {Validator.require})
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

    private String id;

    public WorkshopSection() {
        this.id = UUID.randomUUID().toString();
    }

    public void addWidget(WorkshopWidget widget) {
        if (widgets == null) {
            widgets = new ArrayList<>();
        }
        widgets.add(widget);
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @TISExtension
    public static class DefaultDescriptor extends Descriptor<WorkshopSection> {
        @Override
        public String getDisplayName() {
            return "Workshop Section";
        }
    }
}

