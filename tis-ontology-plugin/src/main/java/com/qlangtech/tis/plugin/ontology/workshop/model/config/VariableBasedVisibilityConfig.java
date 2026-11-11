package com.qlangtech.tis.plugin.ontology.workshop.model.config;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;

import java.io.Serializable;

/**
 * Overlay 基于变量的可见性配置抽象基类（支持多态）。
 *
 * <p>改造前 {@code VariableBasedVisibility} 是一个普通 POJO，「不启用」只能靠字段留空
 * （{@code null}）表达 —— 表单里无法显式选择「关闭」，类型层面也不存在「关闭」状态。
 * 现在改为抽象 Describable：
 * <ul>
 *   <li>{@link NoneVariableBasedVisibility} —— 零字段，表示不启用；</li>
 *   <li>{@link VariableBasedVisibility} —— 默认实现，引用变量 + 比较表达式 + 比较值。</li>
 * </ul>
 *
 * <p>本类<b>刻意不声明任何 {@code @FormField} 字段</b>：字段全在子类里。ordinal 在同一次
 * {@code PropertyType.buildPropertyTypes} 里排序，基类与子类各声明一份会撞号。
 * 也因此本类不需要自己的 {@code .json}。
 *
 * <p>同一取舍见同包的 {@code VariableInterfaceConfig}、{@code VariableRoutingConfig}、
 * {@code VariableStateSavingConfig}，以及 {@code workshop.model.ConditionalVisibilityConfig}。
 *
 * <p><b>存量数据</b>：改造前该字段落盘的 {@code <variableBasedVisibility>} 元素不带
 * {@code class=} 属性。抽象字段类型下这种元素无法确定实现类 —— 若某环境手工写过该元素，
 * 需删掉它后重新保存一次。改造时已核实仓库与 {@code cfg_repo} 内无此类存量，故未加兼容转换器。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/11/11
 */
public abstract class VariableBasedVisibilityConfig
        implements Describable<VariableBasedVisibilityConfig>, Serializable {

    private static final long serialVersionUID = 1L;

    protected abstract static class BasicDescriptor extends Descriptor<VariableBasedVisibilityConfig> {
        // 公共描述符逻辑
    }
}
