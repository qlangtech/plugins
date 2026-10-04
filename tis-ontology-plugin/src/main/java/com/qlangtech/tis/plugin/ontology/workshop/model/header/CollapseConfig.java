package com.qlangtech.tis.plugin.ontology.workshop.model.header;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;

import java.io.Serializable;

/**
 * Workshop Header 的折叠配置 —— 不可折叠 / 可折叠二选一。
 *
 * <p>「是否可折叠」由<b>子类类型</b>承担，而非实例字段：{@link CollapsibleConfig}
 * 表示可折叠，{@link NoneCollapseConfig} 表示不可折叠。这样
 * {@code collapsedByDefault} 与 {@code collapsedImage} 不可能出现在「不可折叠」的
 * 实例上，自相矛盾的数据在类型层面即不可表示。改造前恰恰是
 * {@code collapsible} + {@code collapsedByDefault} + {@code collapsedImage} 三个平铺
 * 字段，能构造出「不可折叠却带着折叠图标 URL」这种状态。
 *
 * <p>折叠与方向是<b>正交</b>的两个维度（水平顶栏同样可以折叠），
 * 因此本类挂在 {@code WorkshopHeader} 顶层，而不是塞进
 * {@link VerticalOrientation} 里。
 *
 * <p>本类<b>刻意不声明任何 {@code @FormField} 字段</b>（原因同
 * {@link HeaderOrientation}），因此也没有自己的 {@code .json}。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/20
 */
public abstract class CollapseConfig implements Describable<CollapseConfig>, Serializable {

    private static final long serialVersionUID = 1L;

    public abstract static class BasicDescriptor extends Descriptor<CollapseConfig> {
    }
}
