package com.qlangtech.tis.plugin.ontology.workshop.model;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.OneStepOfMultiSteps;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.workshop.enums.RecomputeBehavior;
import com.qlangtech.tis.plugin.ontology.workshop.model.config.VariableInterfaceConfig;
import com.qlangtech.tis.plugin.ontology.workshop.model.config.VariableRoutingConfig;
import com.qlangtech.tis.plugin.ontology.workshop.model.config.VariableStateSavingConfig;
import com.qlangtech.tis.plugin.ontology.workshop.model.definition.VariableDefinitionConfig;
import com.qlangtech.tis.util.IPluginContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Workshop Variable 多步配置的<b>第二步</b>：值的计算方式与运行时行为。
 *
 * <p>本步是「值怎么算」的全部内容，其中的 {@link #definitionConfig} 是本步与第一步
 * {@link MetadataOfVariable#type} 存在依赖的字段：定义方式的候选集要按第一步所选类型收敛，
 * 见 {@link #descFilter}。
 *
 * <p>其余四个字段（{@link #recomputeBehavior} / {@link #lazyLoading} /
 * {@link #interfaceConfig} / {@link #routingConfig} / {@link #stateSavingConfig}）
 * 与类型无关，当初与 {@code definitionConfig} 同处一张表单，搬过来只是因为没有理由让
 * 第一步再多背四个字段。
 *
 * @see MetadataOfVariable
 * @see WorkshopVariable
 */
public class DefinitionOfVariable extends OneStepOfMultiSteps {

    /**
     * 值怎么算，见 {@link VariableDefinitionConfig}。
     * 具体子类的 Java 类型<em>即是</em>「定义类型」本身，因此没有平行的类型字段。
     */
    @FormField(ordinal = 0, validate = {Validator.require})
    public VariableDefinitionConfig definitionConfig;

    /**
     * 依赖传播时是否自动重算，仅 AUTOMATIC 会随上游变更自动传播
     */
    @FormField(ordinal = 1, type = FormFieldType.ENUM)
    public RecomputeBehavior recomputeBehavior = RecomputeBehavior.AUTOMATIC;

    /**
     * 是否延迟加载变量值，直到实际使用时才计算
     */
    @FormField(ordinal = 2, type = FormFieldType.ENUM)
    public Boolean lazyLoading = false;

    /**
     * 变量与外部接口的输入映射（模块间变量传递）
     */
    @FormField(ordinal = 3)
    public VariableInterfaceConfig interfaceConfig;

    /**
     * 变量值变化后的页面跳转行为
     */
    @FormField(ordinal = 4)
    public VariableRoutingConfig routingConfig;

    /**
     * 变量值的持久化策略
     */
    @FormField(ordinal = 5)
    public VariableStateSavingConfig stateSavingConfig;

    /**
     * 按第一步 {@link MetadataOfVariable#type} 过滤本步可用的定义方式，供表单侧
     * {@code subDescEnumFilter} 钩子直接调用（接线见 {@code DefinitionOfVariable.json}）。
     *
     * <p>作用点是 {@code PropertyType.applicableDescriptors(boolean)}
     * （在表单元数据构建期对本字段的全部候选 impl 应用一段 Groovy 函数）。
     * 这与改造前 {@code WorkshopVariable.json} 从未接线、两个控件互不相关形成了对比——
     * 那时用户能在同一张表单里选出「数值类型的对象集合定义」，只能靠落盘前的
     * {@code WorkshopVariable.DefaultDescriptor#validateDefinition} 兜底报错。
     *
     * <p>注意本过滤<b>只是体验</b>：{@code validateDefinition} 仍是权威的落盘守卫，
     * 绕过前端直接 POST 非法组合一样会被拒绝。
     *
     * @param descs 本字段的全部候选 descriptor（由框架下发）
     * @return 与第一步类型兼容的候选；无上下文时即 {@code descs} 本身
     */
    public static List<Descriptor> descFilter(List<Descriptor> descs) {
        Optional<MetadataOfVariable> metaStepOpt = getMetaStepQuietly();
        if (metaStepOpt.isEmpty() || metaStepOpt.get().type == null) {
            // 上下文缺失（如 appendExternalProps 预生成元数据、LLM schema 生成场景），
            // 或用户还没选类型：把全部候选原样返回，不制造「选了类型却没得选」的死角
            return descs;
        }
        List<Class<? extends VariableDefinitionConfig.BasicDescriptor>> supported =
                VariableDefinitionConfig.definitionsOf(metaStepOpt.get().type);
        List<Descriptor> subDescs = new ArrayList<>(descs.size());
        for (Descriptor desc : descs) {
            if (supported.contains(desc.getClass())) {
                subDescs.add(desc);
            }
        }
        return subDescs;
    }

    /**
     * 本步 {@code definitionConfig} 的动态 help 文案。
     *
     * @see com.qlangtech.tis.plugin.ontology.impl.valuetype.ConstraintsOfValueType#generateConstraintHelp()
     */
    public static String generateDefinitionHelp() {
        return getMetaStepQuietly()
                .map(metaStep -> metaStep.type == null
                        ? "请先在上一步选择变量类型"
                        : "为'" + metaStep.type.shortComment() + "'类型选择定义方式")
                .orElse("变量值的计算方式由所选配置类型决定");
    }

    /**
     * 安静版获取第一步的实例：上下文 / step1 任一缺失时返回 {@link Optional#empty()}，不抛异常。
     *
     * <p>step1 实例是在 {@code OneStepOfMultiSteps#processCurrentStep} 里
     * {@code currentCtx.put(this.getClass().getName(), this)} 放进去的，
     * 这里按同样的 key 取回。
     *
     * @see com.qlangtech.tis.plugin.ontology.impl.valuetype.ConstraintsOfValueType
     */
    private static Optional<MetadataOfVariable> getMetaStepQuietly() {
        IPluginContext threadLocalCtx = IPluginContext.pluginContextThreadLocal.get();
        if (threadLocalCtx == null) {
            return Optional.empty();
        }
        Context context;
        try {
            context = threadLocalCtx.getContext();
        } catch (UnsupportedOperationException e) {
            return Optional.empty();
        }
        if (context == null) {
            return Optional.empty();
        }
        return Optional.ofNullable((MetadataOfVariable) context.get(MetadataOfVariable.class.getName()));
    }

    @TISExtension
    public static class Desc extends BasicDesc {

        @Override
        public String getStepDescription() {
            return "Definition";
        }

        @Override
        public Step getStep() {
            return Step.Step2;
        }

        @Override
        public Optional<BasicDesc> nextPluginDesc(OneStepOfMultiSteps current) {
            return Optional.empty();
        }

        @Override
        public boolean isFinalStep() {
            return true;
        }
    }
}
