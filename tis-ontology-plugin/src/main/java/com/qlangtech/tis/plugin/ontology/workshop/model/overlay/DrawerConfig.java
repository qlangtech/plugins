package com.qlangtech.tis.plugin.ontology.workshop.model.overlay;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.manage.common.Option;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;

import java.util.List;

/**
 * Drawer 类型配置
 * 从屏幕边缘滑出的侧边面板
 */
public class DrawerConfig extends OverlayTypeConfig {

  @FormField(ordinal = 0, type = FormFieldType.INT_NUMBER, validate = {Validator.require})
  public Integer width = 480;

  @FormField(ordinal = 1, type = FormFieldType.SELECTABLE, validate = {Validator.require})
  public String placement = "right";

  @FormField(ordinal = 2, type = FormFieldType.ENUM)
  public Boolean closable = true;

  @FormField(ordinal = 3, type = FormFieldType.ENUM)
  public Boolean maskClosable = true;

  @TISExtension
  public static class DefaultDescriptor extends BasicDescriptor implements DescriptorUseableShortComment {

    public DefaultDescriptor() {
      super();
      this.registerSelectOptions("placement", () -> List.of(
        new Option("左侧", "left"),
        new Option("右侧", "right"),
        new Option("顶部", "top"),
        new Option("底部", "bottom")
      ));
    }

    @Override
    public String getDisplayName() {
      return "Drawer";
    }

    @Override
    public String shortComment() {
      return "抽屉面板";
    }
  }
}