package com.qlangtech.tis.plugin.ontology.workshop.model.definition;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;

/**
 * 静态值配置
 * 变量直接由用户指定的固定值决定
 */
public class StaticConfig extends VariableDefinitionConfig {

  @FormField(ordinal = 0, type = FormFieldType.TEXTAREA, validate = {Validator.require})
  public String value;

  /** 静态值无需计算，前端直接取用 */
  @Override
  public boolean isBackendComputed() {
    return false;
  }

  @TISExtension
  public static class DefaultDescriptor extends BasicDescriptor implements DescriptorUseableShortComment {
    @Override
    public String getDisplayName() {
      return "Static";
    }

    @Override
    public String shortComment() {
      return "静态值";
    }
  }
}