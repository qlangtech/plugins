package com.qlangtech.tis.plugin.ontology.workshop.model.definition;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;

import java.io.Serializable;

/**
 * 参数配置
 * 用于 FunctionConfig 和 SQLQueryConfig 中定义参数与变量的绑定关系
 *
 * <p>parameter — 参数名，传递给 Function 执行引擎（FunctionConfig）或 SQL 查询（SQLQueryConfig）中的参数名称。
 * 对于 FunctionConfig，对应函数签名中的参数名称；对于 SQLQueryConfig，对应 SQL 模板中的 :paramName 占位符。</p>
 *
 * <p>variableId — 提供参数值的源变量 ID，指向同一个 Workshop Module 中其他
 * {@link com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopVariable} 实例的
 * <code>id</code>（UUID 字符串）。系统通过此 ID 获取该变量的当前值，作为参数传入
 * Function 执行引擎或 SQL 查询。这构成了变量间的<em>依赖关系</em>——当前变量依赖其 parameters/inputs
 * 中引用的上游变量，依赖图服务会据此构建 DAG 进行拓扑排序和自动重算。</p>
 */
public class FunctionParamConfig implements Describable<FunctionParamConfig>, Serializable {

  private static final long serialVersionUID = 1L;

  @FormField(ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
  public String parameter;

  @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
  public String variableId;

  @TISExtension
  public static class DefaultDescriptor extends Descriptor<FunctionParamConfig> {
    @Override
    public String getDisplayName() {
      return "Param";
    }
  }
}