package com.qlangtech.tis.plugin.ontology.workshop.model.header;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;

import java.io.Serializable;

/**
 * Workshop Header 的应用 Logo（Palantir 的 "Enable an application logo"）——
 * 图标 / 图片二选一。
 *
 * <p>「用哪种 Logo」由<b>子类类型</b>承担：{@link IconLogoConfig} 表示图标，
 * {@link ImageLogoConfig} 表示图片。这样「图标颜色」与「图片高度/位置」各自的载荷
 * 不可能出现在对方形态的实例上，自相矛盾的数据在类型层面即不可表示。
 *
 * <p>本类<b>刻意不声明任何 {@code @FormField} 字段</b>（原因同
 * {@link HeaderOrientation}），因此也没有自己的 {@code .json}。
 *
 * <p>宿主字段 {@code WorkshopHeader.logoConfig} 可空（无 {@code require}）：
 * 不配置 = 不显示 logo，即 Palantir 的「Enable logo」开关语义。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/11/14
 */
public abstract class LogoConfig implements Describable<LogoConfig>, Serializable {

    private static final long serialVersionUID = 1L;

    public abstract static class BasicDescriptor extends Descriptor<LogoConfig> {
    }
}
