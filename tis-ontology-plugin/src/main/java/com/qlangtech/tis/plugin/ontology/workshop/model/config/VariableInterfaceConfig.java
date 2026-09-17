package com.qlangtech.tis.plugin.ontology.workshop.model.config;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;

/**
 * Workshop Variable 接口配置抽象基类 —— 变量与外部接口的交互方式。
 *
 * <p>「是否配置」由<b>子类类型</b>承担，而非实例字段：{@link MappingInterfaceConfig}
 * 表示开启，{@link NoneInterfaceConfig} 表示不启用。这样 {@code interfaceId} 与
 * {@code inputs} 不可能出现在「关闭」的实例上，自相矛盾的数据在类型层面即不可表示。
 * 同一取舍见 {@code workshop.model.definition.VariableDefinitionConfig} 的类注释。
 *
 * <p>本类<b>刻意不声明任何 {@code @FormField} 字段</b>：字段全在「开启」子类里。
 * 原因是 ordinal 在同一次 {@code PropertyType.buildPropertyTypes} 里排序，
 * 基类与子类各声明一份会撞号（重复 ordinal 的排序不确定）。
 * 也因此本类不需要自己的 {@code .json}。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/16
 */
public abstract class VariableInterfaceConfig implements Describable<VariableInterfaceConfig> {

    protected abstract static class BasicDescriptor extends Descriptor<VariableInterfaceConfig> {
        // 公共描述符逻辑
    }
}
