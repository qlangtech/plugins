package com.qlangtech.tis.plugin.ontology.workshop.model;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 单个指标配置
 */
public class MetricConfig implements Describable<MetricConfig>, Serializable {

  private static final long serialVersionUID = 1L;

  @FormField(ordinal = 0, type = FormFieldType.INPUTTEXT)
  public String label;

  @FormField(ordinal = 1, type = FormFieldType.TEXTAREA)
  public String description;

  @FormField(ordinal = 2, type = FormFieldType.SELECTABLE, validate = {Validator.require})
  public ValueType valueType = ValueType.NUMBER;

  @FormField(ordinal = 3, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
  public String valueVariable; // 变量 ID

  @FormField(ordinal = 4)
  public NumericFormatting numericFormatting;

  @FormField(ordinal = 5, type = FormFieldType.MULTI_SELECTABLE)
  public List<ConditionalFormatRule> conditionalFormatting = new ArrayList<>();

  @FormField(ordinal = 6, type = FormFieldType.ENUM)
  public Boolean showSecondaryMetric = false;

  @FormField(ordinal = 7)
  public MetricConfig secondaryMetric;

  @FormField(ordinal = 8, type = FormFieldType.ENUM)
  public Boolean showVisualization = false;

  @FormField(ordinal = 9)
  public TimeSeriesVisualization visualization;

  @TISExtension
  public static class DefaultDescriptor extends Descriptor<MetricConfig> {
    @Override
    public String getDisplayName() {
      return "Metric Config";
    }
  }

  public enum ValueType implements DescriptorUseableShortComment {
    STRING("字符串"),
    NUMBER("数值");

    private final String comment;

    ValueType(String comment) {
      this.comment = comment;
    }

    @Override
    public String shortComment() {
      return comment;
    }
  }
}
