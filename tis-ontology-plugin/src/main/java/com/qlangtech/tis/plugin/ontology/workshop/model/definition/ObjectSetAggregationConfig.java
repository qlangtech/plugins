package com.qlangtech.tis.plugin.ontology.workshop.model.definition;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.manage.common.Option;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;

import java.util.List;

/**
 * 对象集合聚合配置
 * 变量值通过对一个 Object Set 进行聚合计算得出
 */
public class ObjectSetAggregationConfig extends VariableDefinitionConfig {

  @FormField(ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
  public String objectSetVariableId;

  @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
  public String propertyType;

  @FormField(ordinal = 2, type = FormFieldType.SELECTABLE, validate = {Validator.require})
  public String aggregationType; // count|sum|average|min|max|approx-unique

  /** 大规模聚合放后端，避免前端内存压力 */
  @Override
  public boolean isBackendComputed() {
    return true;
  }

  @TISExtension
  public static class DefaultDescriptor extends BasicDescriptor implements DescriptorUseableShortComment {

    public DefaultDescriptor() {
      super();
      this.registerSelectOptions("aggregationType", () -> List.of(
        new Option("计数", "count"),
        new Option("求和", "sum"),
        new Option("平均值", "average"),
        new Option("最小值", "min"),
        new Option("最大值", "max"),
        new Option("近似唯一值", "approx-unique")
      ));
    }

    @Override
    public String getDisplayName() {
      return "ObjectSet Aggregation";
    }

    @Override
    public String shortComment() {
      return "集合聚合";
    }
  }
}