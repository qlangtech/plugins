package com.qlangtech.tis.plugin.ontology.workshop.model;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;

import java.io.Serializable;

/**
 * 数值格式化配置
 *
 * <p>被 {@link AxisConfig#numericFormatting}（轴的数值格式化）与 {@code MetricConfig}
 * （指标卡的数值格式化）共用。
 *
 * <h3>本轮修正</h3>
 * <ul>
 *   <li>{@code notation} 由 {@code SELECTABLE} 改为 {@code ENUM} —— 四个取值完全固定，
 *       属于滥用 SELECTABLE（后者只应用于选项来自运行时外部数据的场景）</li>
 *   <li>{@code unit} 的 {@code @FormField} 类型由 {@code INT_NUMBER} 改为
 *       {@code INPUTTEXT} —— 该字段声明为 {@code String}，与 {@code INT_NUMBER}
 *       （对应 JSON Schema 的 integer）自相矛盾</li>
 * </ul>
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/15
 */
public class NumericFormatting implements Describable<NumericFormatting>, Serializable {

  private static final long serialVersionUID = 1L;

  /** 是否启用千分位分组 */
  @FormField(ordinal = 0, type = FormFieldType.ENUM)
  public Boolean useGrouping = true;

  /** 展示的最少小数位数 */
  @FormField(ordinal = 1, type = FormFieldType.INT_NUMBER)
  public Integer minFractionDigits = 0;

  /** 展示的最多小数位数 */
  @FormField(ordinal = 2, type = FormFieldType.INT_NUMBER)
  public Integer maxFractionDigits = 2;

  /** 记数法 */
  @FormField(ordinal = 3, type = FormFieldType.ENUM)
  public Notation notation = Notation.STANDARD;

  /** 单位后缀（如 %、ms、元），可为空 */
  @FormField(ordinal = 4, type = FormFieldType.INPUTTEXT, advance = true)
  public String unit;

  @TISExtension
  public static class DefaultDescriptor extends Descriptor<NumericFormatting> {
    @Override
    public String getDisplayName() {
      return "Numeric Formatting";
    }
  }

  /**
   * 记数法。
   */
  public enum Notation implements DescriptorUseableShortComment {
    STANDARD("标准"),
    COMPACT("紧凑"),
    SCIENTIFIC("科学计数"),
    ENGINEERING("工程计数");

    private final String comment;

    Notation(String comment) {
      this.comment = comment;
    }

    @Override
    public String shortComment() {
      return comment;
    }
  }
}
