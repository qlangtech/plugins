package com.qlangtech.tis.plugin.ontology.workshop.model.definition;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;

/**
 * 对象集合定义配置
 * 变量值通过 Ontology 查询获取的对象集合
 */
public class ObjectSetDefinitionConfig extends VariableDefinitionConfig {

  @FormField(ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
  public String objectType;

  @FormField(ordinal = 1, type = FormFieldType.TEXTAREA)
  public String filters; // JSON array: [{ "property": "string", "operator": "string", "value": "any" }]

  /** 通过 Ontology 查询服务取对象集合，前端直接发起 */
  @Override
  public boolean isBackendComputed() {
    return false;
  }

  @TISExtension
  public static class DefaultDescriptor extends BasicDescriptor implements DescriptorUseableShortComment {
    @Override
    public String getDisplayName() {
      return "ObjectSet Definition";
    }

    @Override
    public String shortComment() {
      return "对象集合定义";
    }
  }
}