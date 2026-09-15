package com.qlangtech.tis.plugin.ontology.workshop.model;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.workshop.model.page.PageTemplate;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Workshop Page 实体
 * <p>
 * sections 使用 transient 修饰，不随 WorkshopPage 主 JSON 序列化，
 * 由 WorkshopSection + WorkshopWidget 等子实体独立存储。
 */
public class WorkshopPage implements Describable<WorkshopPage>, Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 唯一标识，@FormField(identity = true) 用于 IPluginStore.setPlugins()
     * 区分实例。advance = true 将此字段推至"高级"分组，不在基本表单中显示。
     */
    @FormField(identity = true, ordinal = -1, type = FormFieldType.INPUTTEXT)
    public String id;

    @FormField(ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String name;

    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT)
    public String displayName;

    @FormField(ordinal = 2, type = FormFieldType.ENUM, validate = {Validator.require})
    public PageTemplate template = PageTemplate.BLANK;

    @FormField(ordinal = 3, type = FormFieldType.INT_NUMBER)
    public Integer sortOrder = 0;

    // @FormField(ordinal = 10, type = FormFieldType.MULTI_SELECTABLE)
    public transient List<WorkshopSection> sections = new ArrayList<>();

    public WorkshopPage() {
        this.id = UUID.randomUUID().toString();
    }

    public void addSection(WorkshopSection section) {
        if (sections == null) {
            sections = new ArrayList<>();
        }
        sections.add(section);
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @TISExtension
    public static class DefaultDescriptor extends Descriptor<WorkshopPage> {
        @Override
        public String getDisplayName() {
            return "Workshop Page";
        }
    }
}

