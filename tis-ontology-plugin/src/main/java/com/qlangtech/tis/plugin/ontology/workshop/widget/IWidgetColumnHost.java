package com.qlangtech.tis.plugin.ontology.workshop.widget;

import java.util.List;

/**
 * 承载 {@link WidgetColumnConfig} 子表单的宿主 Widget。
 *
 * <p>{@code WidgetColumnConfig.DftDescriptor} 被 4 个宿主共享，而这些宿主的字段名并不统一：
 * <ul>
 *   <li>对象集合变量绑定字段：{@code ObjectTableWidget} / {@code ObjectListWidget} 是
 *       {@code objectSetVar}，{@code PropertyListWidget} / {@code ObjectViewWidget} 是 {@code objectVar}</li>
 *   <li>列配置列表字段：{@code columns} / {@code cardFields} / {@code properties}</li>
 * </ul>
 * 本接口把「绑定哪个对象集合变量」与「当前已保存的列」这两个事实收敛成一个契约，
 * 使 descriptor 不必写 instanceof 链去逐个识别宿主类型。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/4
 */
public interface IWidgetColumnHost {

    /**
     * 宿主绑定的对象集合变量名（即各自字段的原始值，可能为 null / 空）。
     */
    String getBoundObjectSetVar();

    /**
     * 宿主当前已保存的列/行配置，可能为 null。
     */
    List<WidgetColumnConfig> getColumnConfigs();
}
