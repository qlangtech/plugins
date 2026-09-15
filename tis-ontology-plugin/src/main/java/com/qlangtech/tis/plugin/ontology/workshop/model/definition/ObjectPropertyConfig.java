package com.qlangtech.tis.plugin.ontology.workshop.model.definition;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;

/**
 * 对象属性配置
 * 变量值从某个 Object 变量中提取指定属性
 */
public class ObjectPropertyConfig extends VariableDefinitionConfig {

  @FormField(ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
  public String objectVariableId;

  @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
  public String propertyName;

  /** 从上游变量值中取属性，纯前端操作 */
  @Override
  public boolean isBackendComputed() {
    return false;
  }

  @TISExtension
  public static class DefaultDescriptor extends BasicDescriptor implements DescriptorUseableShortComment {
    @Override
    public String getDisplayName() {
      return "Object Property";
    }

    @Override
    public String shortComment() {
      return "对象属性";
    }
  }
}