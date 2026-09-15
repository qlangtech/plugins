package com.qlangtech.tis.plugin.ontology.workshop.model.definition;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;

import java.util.List;

/**
 * SQL 查询配置
 * 变量值通过执行 Ontology SQL 查询得到
 *
 * <p>parameters 列表中的每个 {@link FunctionParamConfig} 定义一个参数绑定：
 * <ul>
 *   <li><b>parameter</b> — SQL 模板中的参数名，对应 SQL 模板中的 <code>:paramName</code> 占位符</li>
 *   <li><b>variableId</b> — 提供参数值的源变量 ID，指向同一个 Workshop Module 中其他
 *       {@link com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopVariable} 实例的
 *       <code>id</code>（UUID 字符串）。系统通过此 ID 获取该变量的当前值，代入 SQL 查询执行。
 *       这构成了变量间的<em>依赖关系</em>——SQL 变量依赖其 parameters 中引用的上游变量，
 *       依赖图服务会据此构建 DAG 进行拓扑排序和自动重算。</li>
 * </ul>
 *
 * 运行时行为（参见 VariableComputationService.computeSQLQuery()）：
 * <ol>
 *   <li>遍历 parameters 列表，对每个 {@link FunctionParamConfig} 提取 parameter 和 variableId</li>
 *   <li>通过 variableId 从 context 中获取源变量的当前值（若尚未计算则递归计算）</li>
 *   <li>以 parameter 为键、变量值为值，构建参数 Map</li>
 *   <li>将参数注入 sqlTemplate 并执行 ontologyService.executeSQL()</li>
 * </ol>
 */
public class SQLQueryConfig extends VariableDefinitionConfig {

  @FormField(ordinal = 0, type = FormFieldType.TEXTAREA, validate = {Validator.require})
  public String sqlTemplate;

  @FormField(ordinal = 1)
  public List<FunctionParamConfig> parameters;

  /** SQL 在后端执行，需安全隔离避免注入 */
  @Override
  public boolean isBackendComputed() {
    return true;
  }

  @TISExtension
  public static class DefaultDescriptor extends BasicDescriptor implements DescriptorUseableShortComment {
    @Override
    public String getDisplayName() {
      return "SQL Query";
    }

    @Override
    public String shortComment() {
      return "SQL查询";
    }
  }
}