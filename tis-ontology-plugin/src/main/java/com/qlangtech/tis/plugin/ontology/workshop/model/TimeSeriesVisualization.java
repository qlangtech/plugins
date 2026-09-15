package com.qlangtech.tis.plugin.ontology.workshop.model;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;

import java.io.Serializable;

/**
 * 时间序列可视化配置
 */
public class TimeSeriesVisualization implements Describable<TimeSeriesVisualization>, Serializable {

  private static final long serialVersionUID = 1L;

  @FormField(ordinal = 0, type = FormFieldType.SELECTABLE)
  public VisualizationPosition position = VisualizationPosition.SIDE_BY_SIDE;

  @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
  public String timeSeriesSet; // 时间序列变量 ID

  @FormField(ordinal = 2)
  public TimeRangeConfig timeRange;

  @FormField(ordinal = 3, type = FormFieldType.ENUM)
  public Boolean showBaseline = false;

  @TISExtension
  public static class DefaultDescriptor extends Descriptor<TimeSeriesVisualization> {
    @Override
    public String getDisplayName() {
      return "Time Series Visualization";
    }
  }

  public enum VisualizationPosition implements DescriptorUseableShortComment {
    SIDE_BY_SIDE("并排显示"),
    STACKED("堆叠显示");

    private final String comment;

    VisualizationPosition(String comment) {
      this.comment = comment;
    }

    @Override
    public String shortComment() {
      return comment;
    }
  }
}
