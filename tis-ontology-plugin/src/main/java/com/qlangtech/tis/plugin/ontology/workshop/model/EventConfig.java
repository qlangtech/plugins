package com.qlangtech.tis.plugin.ontology.workshop.model;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;

import java.io.Serializable;

/**
 * 事件配置
 */
public class EventConfig implements Describable<EventConfig>, Serializable {

  private static final long serialVersionUID = 1L;

  @FormField(ordinal = 0, type = FormFieldType.SELECTABLE)
  public EventType eventType;

  @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT)
  public String targetVariable; // 目标变量 ID

  @FormField(ordinal = 2, type = FormFieldType.INPUTTEXT)
  public String actionId; // Ontology Action ID

  @TISExtension
  public static class DefaultDescriptor extends Descriptor<EventConfig> {
    @Override
    public String getDisplayName() {
      return "Event Config";
    }
  }

  public enum EventType implements DescriptorUseableShortComment {
    NAVIGATE("导航"),
    SET_VARIABLE("设置变量"),
    TRIGGER_ACTION("触发动作"),
    OPEN_OVERLAY("打开浮层"),
    CLOSE_OVERLAY("关闭浮层");

    private final String comment;

    EventType(String comment) {
      this.comment = comment;
    }

    @Override
    public String shortComment() {
      return comment;
    }
  }
}
