package com.qlangtech.tis.plugin.ontology.workshop.model.config;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;

/**
 * Workshop Variable 路由配置抽象基类 —— 变量值变化后的页面跳转行为。
 *
 * <p>「是否配置」由<b>子类类型</b>承担：{@link PageRoutingConfig} 表示开启，
 * {@link NoneRoutingConfig} 表示不启用。理由同 {@link VariableInterfaceConfig}。
 *
 * <p>本类不声明 {@code @FormField} 字段，故不需要自己的 {@code .json}。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/16
 */
public abstract class VariableRoutingConfig implements Describable<VariableRoutingConfig> {

    protected abstract static class BasicDescriptor extends Descriptor<VariableRoutingConfig> {
        // 公共描述符逻辑
    }
}
