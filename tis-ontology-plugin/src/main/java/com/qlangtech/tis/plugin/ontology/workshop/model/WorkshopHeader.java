package com.qlangtech.tis.plugin.ontology.workshop.model;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;

import java.io.Serializable;

/**
 * Workshop Header 配置
 */
public class WorkshopHeader implements Describable<WorkshopHeader>, Serializable {


  @FormField(ordinal = 0, type = FormFieldType.ENUM)
  public Boolean visible = true;

  @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT)
  public String title;

  @FormField(ordinal = 2, type = FormFieldType.INPUTTEXT)
  public String titleColor;

  @FormField(ordinal = 3, type = FormFieldType.SELECTABLE)
  public HeaderOrientation orientation = HeaderOrientation.HORIZONTAL;

  @FormField(ordinal = 4, type = FormFieldType.INT_NUMBER)
  public Integer height;  // px (horizontal mode)

  @FormField(ordinal = 5, type = FormFieldType.INT_NUMBER)
  public Integer width;   // px (vertical mode)

  @FormField(ordinal = 6, type = FormFieldType.ENUM)
  public Boolean collapsible = false;

  @FormField(ordinal = 7, type = FormFieldType.ENUM)
  public Boolean collapsedByDefault = false;

  @FormField(ordinal = 8, type = FormFieldType.INPUTTEXT)
  public String collapsedImage;

  @FormField(ordinal = 9, type = FormFieldType.INPUTTEXT)
  public String backgroundColor;

  @FormField(ordinal = 10, type = FormFieldType.ENUM)
  public Boolean favoriteEnabled = true;

  @TISExtension
  public static class DefaultDescriptor extends Descriptor<WorkshopHeader> {
    @Override
    public String getDisplayName() {
      return "Workshop Header";
    }
  }

  public enum HeaderOrientation {
    HORIZONTAL, VERTICAL
  }
}
