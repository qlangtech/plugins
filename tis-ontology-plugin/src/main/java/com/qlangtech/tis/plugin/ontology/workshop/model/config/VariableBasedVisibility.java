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

  /** 用于判断的变量 ID */
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