package com.qlangtech.tis.plugin.ontology.workshop.model;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;

import java.io.Serializable;

/**
 * 条件格式化规则
 */
public class ConditionalFormatRule implements Describable<ConditionalFormatRule>, Serializable {

  private static final long serialVersionUID = 1L;

  @FormField(ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
  public String condition; // e.g., "value <= 0"

  @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT)
  public String color; // e.g., "red", "#ff0000"

  @FormField(ordinal = 2, type = FormFieldType.INPUTTEXT)
  public String backgroundColor;

  @FormField(ordinal = 3, type = FormFieldType.INPUTTEXT)
  public String icon;

  @TISExtension
  public static class DefaultDescriptor extends Descriptor<ConditionalFormatRule> {
    @Override
    public String getDisplayName() {
      return "Conditional Format Rule";
    }
  }
}
