package com.qlangtech.tis.plugin.ontology.workshop.model.config;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;

/**
 * Workshop Variable 状态保存配置抽象基类 —— 变量值的持久化策略。
 *
 * <p>改造前这里有一个 {@code boolean enabled} 字段，允许出现「{@code enabled == false}
 * 却带着 {@code storageKey}」这种自相矛盾的数据。现在开关由<b>子类类型</b>承担：
 * {@link PersistentStateSavingConfig} 表示开启，{@link NoneStateSavingConfig} 表示不启用，
 * 矛盾态在类型层面即不可表示。
 *
 * <p>本类不声明 {@code @FormField} 字段，故不需要自己的 {@code .json}。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/16
 */
public abstract class VariableStateSavingConfig implements Describable<VariableStateSavingConfig> {

    protected abstract static class BasicDescriptor extends Descriptor<VariableStateSavingConfig> {
        // 公共描述符逻辑
    }
}
