package com.qlangtech.tis.plugin.ontology.workshop.model;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;

import java.io.Serializable;

/**
 * 时间范围配置
 */
public class TimeRangeConfig implements Describable<TimeRangeConfig>, Serializable {

  private static final long serialVersionUID = 1L;

  @FormField(ordinal = 0, type = FormFieldType.ENUM)
  public TimeRangePreset preset = TimeRangePreset.LAST_DAY;

  @FormField(ordinal = 1, type = FormFieldType.DateTime)
  public java.util.Date customStart; // 自定义开始时间

  @FormField(ordinal = 2, type = FormFieldType.DateTime)
  public java.util.Date customEnd; // 自定义结束时间

  @TISExtension
  public static class DefaultDescriptor extends Descriptor<TimeRangeConfig> {
    @Override
    public String getDisplayName() {
      return "Time Range Config";
    }
  }

  public enum TimeRangePreset implements DescriptorUseableShortComment {
    ALL_TIME("全部时间"),
    LAST_HOUR("最近1小时"),
    LAST_DAY("最近1天"),
    LAST_WEEK("最近1周"),
    CUSTOM("自定义");

    private final String comment;

    TimeRangePreset(String comment) {
      this.comment = comment;
    }

    @Override
    public String shortComment() {
      return comment;
    }
  }
}
