package com.qlangtech.tis.plugin.ontology.workshop.enums;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;

/**
 * Workshop Variable 类型枚举
 */
public enum VariableType implements DescriptorUseableShortComment {
  // 标量类型
  STRING("字符串"),
  BOOLEAN("布尔值"),
  DATE("日期"),
  TIMESTAMP("时间戳"),
  NUMERIC("数值"),
  GEOPOINT("地理坐标点"),
  GEOSHAPE("地理形状"),

  // 复合类型
  ARRAY("数组"),
  STRUCT("结构体"),

  // Ontology 类型
  OBJECT_SET("对象集合"),
  OBJECT_SET_FILTER("对象集合过滤器"),
  TIME_SERIES_SET("时间序列集合");

  private final String comment;

  VariableType(String comment) {
    this.comment = comment;
  }

  @Override
  public String shortComment() {
    return comment;
  }

  public boolean isScalar() {
    return this == STRING || this == BOOLEAN || this == DATE ||
           this == TIMESTAMP || this == NUMERIC ||
           this == GEOPOINT || this == GEOSHAPE;
  }

  public boolean isOntologyType() {
    return this == OBJECT_SET || this == OBJECT_SET_FILTER || this == TIME_SERIES_SET;
  }
}