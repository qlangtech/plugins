package com.qlangtech.tis.plugin.ontology.workshop.model.variable;

import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.ontology.workshop.enums.VariableType;
import com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopVariable;

/**
 * {@link VariableType#OBJECT_SET} —— 一组 Ontology 对象，含其属性与关系。
 *
 * <p>本类<b>没有自己的字段</b>：变量类型由 Java 类型承担，其余配置全部继承自
 * {@link WorkshopVariable}。可用哪些定义方式见
 * {@code VariableDefinitionConfig#TYPE_DEFINITIONS} 中本类型对应的那一行
 * —— 不在这里重复，避免矩阵出现第二份会过期的副本。
 */
public class ObjectSetVariable extends WorkshopVariable {

  @Override
  public VariableType getVariableType() {
    return VariableType.OBJECT_SET;
  }

  @TISExtension
  public static class DefaultDescriptor extends BasicDescriptor {

    public DefaultDescriptor() {
      super(VariableType.OBJECT_SET);
    }
  }
}
