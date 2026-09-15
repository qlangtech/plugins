package com.qlangtech.tis.plugin.ontology.workshop.model;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;

import java.io.Serializable;

/**
 * 路由配置
 */
public class RoutingConfig implements Describable<RoutingConfig>, Serializable {

  private static final long serialVersionUID = 1L;

  @FormField(ordinal = 0, type = FormFieldType.ENUM)
  public Boolean enabled = false;

  @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT)
  public String urlPattern;

  @TISExtension
  public static class DefaultDescriptor extends Descriptor<RoutingConfig> {
    @Override
    public String getDisplayName() {
      return "Routing Config";
    }
  }
}
