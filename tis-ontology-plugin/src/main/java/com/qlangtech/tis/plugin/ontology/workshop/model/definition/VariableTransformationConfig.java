package com.qlangtech.tis.plugin.ontology.workshop.model.definition;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.manage.common.Option;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;

import java.util.List;

/**
 * 变量转换配置
 * 变量值通过对另一个变量的值进行简单变换得到
 */
public class VariableTransformationConfig extends VariableDefinitionConfig {

  @FormField(ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
  public String sourceVariableId;

  @FormField(ordinal = 1, type = FormFieldType.SELECTABLE, validate = {Validator.require})
  public String transformationType; // uppercase|lowercase|concat|substring|to-number|to-string|to-boolean

  @FormField(ordinal = 2, type = FormFieldType.TEXTAREA)
  public String parameters; // JSON object

  /** 对上游变量值做字符串/类型变换，纯前端操作 */
  @Override
  public boolean isBackendComputed() {
    return false;
  }

  @TISExtension
  public static class DefaultDescriptor extends BasicDescriptor implements DescriptorUseableShortComment {

    public DefaultDescriptor() {
      super();
      this.registerSelectOptions("transformationType", () -> List.of(
        new Option("大写", "uppercase"),
        new Option("小写", "lowercase"),
        new Option("拼接", "concat"),
        new Option("截取", "substring"),
        new Option("转数字", "to-number"),
        new Option("转字符串", "to-string"),
        new Option("转布尔", "to-boolean")
      ));
    }

    @Override
    public String getDisplayName() {
      return "Variable Transformation";
    }

    @Override
    public String shortComment() {
      return "变量转换";
    }
  }
}