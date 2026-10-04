package com.qlangtech.tis.plugin.ontology.workshop.model.config;

import java.io.Serializable;

/**
 * Workshop 基于变量的可见性配置
 * 用于 Overlay 和 Section 的条件显示控制
 */
public class VariableBasedVisibility implements Serializable {

  private static final long serialVersionUID = 1L;

  /** 可见性表达式（如 "equals", "notEmpty", "greaterThan"） */
  private String expression;

  /**
   * 用于判断的变量 —— <b>存的是变量名，不是 ID</b>。
   *
   * 字段名是历史遗留：{@code WorkshopVariable} 的 id 已删除、identity 字段就是 name。
   * 之所以不把字段名一并改成 variableName —— 它同时是表单的线格式键：
   * 本类经 {@code WorkshopOverlay.variableBasedVisibility}（{@code @FormField(ordinal = 6)}）
   * 作为嵌套表单下发，前端 {@code overlay.model.ts} 的 {@code variableBasedVisibility.variableId}
   * 与 {@code section.model.ts} 的 {@code ConditionalVisibility.variableId} 都按这个键读取，
   * 改名会静默断开这条链路。要改必须两端同批。
   *
   * 注：本字段没有 {@code Descriptor.registerSelectOptions} 供给候选，
   * 目前是让用户手填变量名的普通文本输入 —— 这与「变量绑定应给下拉」的约定不符，
   * 属既有缺口，不在本次 id→name 迁移范围内。
   */
  private String variableId;

  /** 比较值（可选） */
  private String compareValue;

  public String getExpression() {
    return expression;
  }

  public void setExpression(String expression) {
    this.expression = expression;
  }

  public String getVariableId() {
    return variableId;
  }

  public void setVariableId(String variableId) {
    this.variableId = variableId;
  }

  public String getCompareValue() {
    return compareValue;
  }

  public void setCompareValue(String compareValue) {
    this.compareValue = compareValue;
  }

  /**
   * 判断是否可见
   */
  public boolean isVisible(Object variableValue) {
    if (variableValue == null) {
      return false;
    }

    switch (expression) {
      case "equals":
        return compareValue != null && variableValue.toString().equals(compareValue);
      case "notEmpty":
        if (variableValue instanceof String) {
          return !((String) variableValue).isEmpty();
        }
        return true;
      case "greaterThan":
        if (variableValue instanceof Number && compareValue != null) {
          return ((Number) variableValue).doubleValue() > Double.parseDouble(compareValue);
        }
        return false;
      case "lessThan":
        if (variableValue instanceof Number && compareValue != null) {
          return ((Number) variableValue).doubleValue() < Double.parseDouble(compareValue);
        }
        return false;
      case "truthy":
        if (variableValue instanceof Boolean) {
          return (Boolean) variableValue;
        }
        if (variableValue instanceof Number) {
          return ((Number) variableValue).doubleValue() != 0;
        }
        return true;
      default:
        return true;
    }
  }
}