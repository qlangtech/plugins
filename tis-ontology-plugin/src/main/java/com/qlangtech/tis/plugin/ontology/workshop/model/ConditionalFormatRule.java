package com.qlangtech.tis.plugin.ontology.workshop.model;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.IPluginStore;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;

import java.io.Serializable;

/**
 * 条件格式化规则
 * <p>
 * {@code name} 必须是 identity 字段，不能拿 {@code condition} 顶替：identity 值会作为落盘文件名，
 * 而 {@code Validator.identity} 的字符集是 {@code [A-Z\d a-z_\-]+}，
 * {@code "value <= 0"} 这样的表达式含空格与比较符，会被直接拒掉。
 */
public class ConditionalFormatRule implements Describable<ConditionalFormatRule>, Serializable,
        IPluginStore.MultiDescribleElement {

  private static final long serialVersionUID = 1L;

  /** 规则名，仅用于在列表中区分各条规则 */
  @FormField(identity = true, ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
  public String name;

  @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
  public String condition; // e.g., "value <= 0"

  @FormField(ordinal = 2, type = FormFieldType.INPUTTEXT)
  public String color; // e.g., "red", "#ff0000"

  @FormField(ordinal = 3, type = FormFieldType.INPUTTEXT)
  public String backgroundColor;

  @FormField(ordinal = 4, type = FormFieldType.INPUTTEXT)
  public String icon;

  @Override
  public String identityValue() {
    return this.name;
  }

  @TISExtension
  public static class DefaultDescriptor extends Descriptor<ConditionalFormatRule> {
    @Override
    public String getDisplayName() {
      return "Conditional Format Rule";
    }
  }
}
