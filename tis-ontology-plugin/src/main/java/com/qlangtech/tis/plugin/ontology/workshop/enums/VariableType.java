package com.qlangtech.tis.plugin.ontology.workshop.enums;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.plugin.IEndTypeGetter;
import com.qlangtech.tis.plugin.ontology.workshop.model.definition.VariableDefinitionConfig;

/**
 * Workshop Variable 类型枚举
 *
 * <h3>为什么带 endType</h3>
 * 枚举实现了 {@link IEndTypeGetter}，这份图标信息由
 * {@code OptionWithEndType.createEnumOptions} 在把枚举反射成下拉选项时读取：
 * 选项带上 {@code endType}，前端 {@code enum-icon-select} 就会在选项左侧渲染对应图标，
 * 并把 {@code EndType#getDesc()} 作为 comment 展示。没有 endType 的枚举选项
 * 前端不画图标（见 {@code TestWorkshopWidgetEnumPropertyType} 里那条断言）。
 *
 * <p>映射口径：取值尽量落在 {@code EndType.EndTypeCategory.DataType} / {@code Ontology}
 * 这两类上 —— 它们本来就是为本体的类型体系画的图标（{@code OntologyType} 用的就是同一批），
 * 变量类型与本体属性类型在语义上是对齐的。{@link #OBJECT_SET_FILTER} 与
 * {@link #TIME_SERIES_SET} 在 DataType 里没有对应项，取 {@code Icon} 分类里的
 * Ant Design 图标名（{@code filter} / {@code line-chart}）。
 *
 * <p>⚠️ 取值必须是 {@code EndType} 里已声明、且 {@code containICON=false} 时
 * 同时是合法 Ant Design 图标名的那些（理由见 {@code EndType} 中那段注释）。
 *
 * @see VariableDefinitionConfig#TYPE_DEFINITIONS
 */
public enum VariableType implements DescriptorUseableShortComment, IEndTypeGetter {
  // 标量类型
  STRING("字符串", EndType.DataTypeString),
  BOOLEAN("布尔值", EndType.DataTypeBoolean),
  DATE("日期", EndType.DataTypeTime),
  TIMESTAMP("时间戳", EndType.DataTypeTime),
  NUMERIC("数值", EndType.DataTypeDecimal),
  GEOPOINT("地理坐标点", EndType.DataTypeGeo),
  GEOSHAPE("地理形状", EndType.DataTypeGeo),

  // 复合类型
  ARRAY("数组", EndType.DataTypeArray),
  STRUCT("结构体", EndType.DataTypeStruct),

  // Ontology 类型
  OBJECT_SET("对象集合", EndType.OntologyObjectType),
  OBJECT_SET_FILTER("对象集合过滤器", EndType.Filter),
  TIME_SERIES_SET("时间序列集合", EndType.LineChart);

  private final String comment;
  private final EndType endType;

  VariableType(String comment, EndType endType) {
    this.comment = comment;
    this.endType = endType;
  }

  @Override
  public String shortComment() {
    return comment;
  }

  @Override
  public EndType getEndType() {
    return this.endType;
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