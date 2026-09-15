package com.qlangtech.tis.plugin.ontology.workshop.model.definition;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;

import java.util.List;

/**
 * 函数计算配置
 * 变量值由后端 Function 执行引擎计算得出
 *
 * <p>inputs 列表中的每个 {@link FunctionParamConfig} 定义一个参数绑定：
 * <ul>
 *   <li><b>parameter</b> — 传递给 Function 执行引擎的<em>参数名</em>，对应函数签名中的参数名称</li>
 *   <li><b>variableId</b> — 提供参数值的源变量 ID，指向同一个 Workshop Module 中其他
 *       {@link com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopVariable} 实例的
 *       <code>id</code>（UUID 字符串）。系统通过此 ID 获取该变量的当前值，作为参数传入
 *       Function 执行引擎。这构成了变量间的<em>依赖关系</em>——Function 变量依赖其 inputs
 *       中引用的上游变量，依赖图服务会据此构建 DAG 进行拓扑排序和自动重算。</li>
 * </ul>
 *
 * 运行时行为（参见 VariableComputationService.computeFunction()）：
 * <ol>
 *   <li>遍历 inputs 列表，对每个 {@link FunctionParamConfig} 提取 parameter 和 variableId</li>
 *   <li>通过 variableId 从 context 中获取源变量的当前值（若尚未计算则递归计算）</li>
 *   <li>以 parameter 为键、变量值为值，构建参数 Map</li>
 *   <li>调用 functionService.execute(functionId, inputs) 执行计算</li>
 * </ol>
 */
public class FunctionConfig extends VariableDefinitionConfig {

  @FormField(ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
  public String functionId;

  @FormField(ordinal = 1)
  public List<FunctionParamConfig> inputs;

  /** Function 执行引擎在后端，需安全隔离 */
  @Override
  public boolean isBackendComputed() {
    return true;
  }

  @TISExtension
  public static class DefaultDescriptor extends BasicDescriptor implements DescriptorUseableShortComment {
    @Override
    public String getDisplayName() {
      return "Function";
    }

    @Override
    public String shortComment() {
      return "函数计算";
    }
  }
}