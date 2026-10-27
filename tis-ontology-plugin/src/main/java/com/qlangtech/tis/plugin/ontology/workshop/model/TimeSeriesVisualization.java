package com.qlangtech.tis.plugin.ontology.workshop.model;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.WidgetOptionHelper;

/**
 * 时间序列可视化配置（启用实现）。
 *
 * <p>多态体系见父类 {@link AbstractTimeSeriesVisualization}：「不显示」由零字段的子类
 * {@link TimeSeriesVisualizationOff} 表达；本类只在用户显式选择启用时出现。
 */
public class TimeSeriesVisualization extends AbstractTimeSeriesVisualization {

  private static final long serialVersionUID = 1L;

  public static final String KEY_TIME_SERIES_SET = "timeSeriesSet";

  /**
   * 取值集合完全固定，故用 ENUM 而非 SELECTABLE —— SELECTABLE 是给「选项来自运行时外部数据」
   * 用的，用它承载硬编码枚举会让前端在打开表单时去找一个永远不存在的选项供给方。
   */
  @FormField(ordinal = 0, type = FormFieldType.ENUM)
  public VisualizationPosition position = VisualizationPosition.SIDE_BY_SIDE;

  @FormField(ordinal = 1, type = FormFieldType.SELECTABLE, validate = {Validator.require})
  public String timeSeriesSet; // 时间序列变量 ID

  @FormField(ordinal = 2)
  public TimeRangeConfig timeRange;

  @FormField(ordinal = 3, type = FormFieldType.ENUM)
  public Boolean showBaseline = false;

  @TISExtension
  public static class DefaultDescriptor extends BasicDescriptor implements DescriptorUseableShortComment {

    public DefaultDescriptor() {
      super();
      this.registerSelectOptions(KEY_TIME_SERIES_SET, WidgetOptionHelper::getTimeSeriesSetVariableOptions);
    }

    @Override
    public String getDisplayName() {
      return "Time Series Visualization";
    }

    @Override
    public String shortComment() {
      return "每个指标下方的迷你趋势图";
    }
  }

  public enum VisualizationPosition implements DescriptorUseableShortComment {
    SIDE_BY_SIDE("并排显示"),
    STACKED("堆叠显示");

    public final String label;

    VisualizationPosition(String label) {
      this.label = label;
    }

    @Override
    public String shortComment() {
      return this.label;
    }
  }
}
