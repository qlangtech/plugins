package com.qlangtech.tis.plugin.ontology.workshop.model.overlay;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;

/**
 * Modal 类型配置
 * 居中弹窗对话框
 */
public class ModalConfig extends OverlayTypeConfig {

  @FormField(ordinal = 0, type = FormFieldType.INT_NUMBER, validate = {Validator.require})
  public Integer width = 520;

  @FormField(ordinal = 1, type = FormFieldType.ENUM)
  public Boolean closable = true;

  @FormField(ordinal = 2, type = FormFieldType.ENUM)
  public Boolean maskClosable = true;

  @FormField(ordinal = 3, type = FormFieldType.ENUM)
  public Boolean centered = true;

  @TISExtension
  public static class DefaultDescriptor extends BasicDescriptor implements DescriptorUseableShortComment {
    @Override
    public String getDisplayName() {
      return "Modal";
    }

    @Override
    public String shortComment() {
      return "模态弹窗";
    }
  }
}