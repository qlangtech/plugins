package com.qlangtech.tis.plugin.ontology.workshop.model;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.workshop.enums.OverlayType;
import com.qlangtech.tis.plugin.ontology.workshop.model.config.VariableBasedVisibility;
import com.qlangtech.tis.plugin.ontology.workshop.model.overlay.OverlayTypeConfig;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Workshop Overlay 实体
 */
public class WorkshopOverlay implements Describable<WorkshopOverlay>, Serializable {

  private static final long serialVersionUID = 1L;

  @FormField(ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
  public String name;

  @FormField(ordinal = 1, type = FormFieldType.SELECTABLE, validate = {Validator.require})
  public OverlayType type;

  @FormField(ordinal = 2, validate = {Validator.require})
  public OverlayTypeConfig typeConfig;

  @FormField(ordinal = 3, type = FormFieldType.ENUM)
  public Boolean showHeader = true;

  @FormField(ordinal = 4, type = FormFieldType.INPUTTEXT)
  public String title;

  @FormField(ordinal = 5, type = FormFieldType.INPUTTEXT)
  public String icon;

  @FormField(ordinal = 6)
  public VariableBasedVisibility variableBasedVisibility;

  @FormField(ordinal = 7, type = FormFieldType.ENUM)
  public Boolean closeOnBackdropClick = true;

  @FormField(ordinal = 8, type = FormFieldType.ENUM)
  public Boolean addBackgroundBehindOverlay = true;

  @FormField(ordinal = 9, type = FormFieldType.INT_NUMBER)
  public Integer sortOrder = 0;

  @FormField(ordinal = 10, type = FormFieldType.MULTI_SELECTABLE)
  public List<WorkshopSection> sections = new ArrayList<>();

  private String id;

  public WorkshopOverlay() {
    this.id = UUID.randomUUID().toString();
  }

  public void addSection(WorkshopSection section) {
    if (sections == null) {
      sections = new ArrayList<>();
    }
    sections.add(section);
  }

  public String getId() { return id; }
  public void setId(String id) { this.id = id; }

  @TISExtension
  public static class DefaultDescriptor extends Descriptor<WorkshopOverlay> {
    @Override
    public String getDisplayName() {
      return "Workshop Overlay";
    }
  }
}
