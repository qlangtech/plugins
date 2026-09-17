package com.qlangtech.tis.plugin.ontology.workshop.model.definition;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;

/**
 * 外部写入配置：值不由任何「定义」算出，而是由运行时的一方（通常是某个 Widget 的交互）
 * 直接写入变量。
 *
 * <h3>为什么需要它</h3>
 * 其余 7 种定义方式都在回答「值怎么算」——静态值、取对象属性、查 SQL、跑函数……
 * 但有一类变量的值**只可能来自用户操作**，构建期无法、也不该为它指定任何计算规则：
 * 图表图层上的刷选区间（A→B）就是典型。刷选之前该变量没有值，刷选之后值才出现，
 * 且此前的表达式里没有任何一处能预先描述「用户会框选哪个区间」。
 *
 * <p>给这类变量配上 {@link StaticConfig} 是错配：它要求填一个静态值（
 * {@code Validator.require}），于是构建期不得不编一个占位值塞进去；运行时又被前端
 * 的初始化流程把这个占位值算出来写进 valueCache，盖住了真正该由 Widget 写入的位置
 * ——见下方「与前端计算的约定」。{@code ExternalInputConfig} 让「此值由外部写入」
 * 成为一份**显式声明**，而不是靠留空某个字段来暗示。
 *
 * <h3>本配置没有任何字段</h3>
 * 这不是「还没写」，而是不需要：值的形状由**变量类型**决定，不由配置决定。
 * 例如 {@code OBJECT_SET_FILTER} 的值是一组谓词，那个形状是变量类型契约的一部分，
 * 换一种定义方式（{@link VariableTransformationConfig}）产出的也必须是同一形状。
 * 若在这里再落一份字段去描述值形态，就会出现「同一变量类型下两种定义方式的值不一样」
 * 的分裂，消费方无从适配。
 *
 * <p>同理也**没有配套的 .json 资源**：json 是给字段写 label / help / 候选值用的，
 * 类里没有 {@code @FormField} 就没有可描述的东西。同一仓库中
 * {@code ActionButtonWidget} / {@code TabsWidget} 等零字段类同样没有 json。
 *
 * <h3>与前端计算的约定</h3>
 * {@link #isBackendComputed()} 返回 {@code false}，但语义上比「前端计算」更强一层：
 * 前端**既不计算、也不产出任何默认值**，只应在收到外部写入时更新 valueCache。
 *
 * <p>⚠️ 这要求前端 {@code variable.service.ts} 的 {@code computeValueInternal}
 * 为其显式返回 {@code undefined}（空值）。该函数的 {@code default} 分支会抛
 * {@code Unknown definition kind}，而 {@code initializeVariables} 会在模块加载时
 * **急切计算全部非 lazy 变量**——所以前端漏了这个分支不是「该变量没值」，
 * 而是**整个模块加载失败**。这是本定义方式在前后端之间唯一的硬耦合点。
 *
 * @see VariableDefinitionConfig#TYPE_DEFINITIONS
 */
public class ExternalInputConfig extends VariableDefinitionConfig {

  /**
   * 值由外部写入，不存在「计算」这一步，故前端不应为其推导任何值。
   *
   * <p>返回 {@code false} 而非 {@code true}：{@code true} 会让前端把求值委托给后端
   * 计算服务，而外部写入的值只存在于前端 valueCache（不落盘），后端无从计算，
   * 委托过去只会拿回一个空值并把写入时机搅乱。
   */
  @Override
  public boolean isBackendComputed() {
    return false;
  }

  @TISExtension
  public static class DefaultDescriptor extends BasicDescriptor implements DescriptorUseableShortComment {
    @Override
    public String getDisplayName() {
      return "External Input";
    }

    @Override
    public String shortComment() {
      return "外部写入";
    }
  }
}
