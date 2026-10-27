package com.qlangtech.tis.plugin.ontology.workshop.widget.impl.chartxy;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;

import java.io.Serializable;

/**
 * 坐标轴标题配置的抽象基类（off / on 两态多态）。
 *
 * <p>原先是 {@code AxisConfig} 上的 {@code showTitle}（Boolean）+ {@code titleOverride}
 * （String）两个平铺字段：开关关闭时覆写文本照样渲染，且可以把「关了却填了标题」落盘。
 * 改为多态：
 * <ul>
 *   <li>{@link AxisTitleOff} —— 零字段，不显示轴标题；</li>
 *   <li>{@link AxisTitleConfig} —— 显示轴标题，覆写文本必填。</li>
 * </ul>
 *
 * <p>本类零 {@code @FormField}，因此不需要 {@code .json}。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/24
 */
public abstract class AbstractAxisTitleConfig
        implements Describable<AbstractAxisTitleConfig>, Serializable {

    private static final long serialVersionUID = 1L;

    public abstract static class BasicDescriptor extends Descriptor<AbstractAxisTitleConfig> {
    }
}
