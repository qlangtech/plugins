package com.qlangtech.tis.plugin.ontology.workshop.widget.impl;

/**
 * Workshop Widget 分类枚举 —— 调色板分组类别
 * <p>
 * 由 {@link WorkShopWidgetType} 的 {@code category} 属性引用，
 * 在 {@link BaseWidgetDescriptor#getExtractProps()} 中序列化为前端 {@code extractProps.category} 字符串。
 * <p>
 * 前端对应类型 {@code WidgetCategory} 定义于 {@code widget-metadata.model.ts}，
 * 与 {@link WidgetPaletteComponent#categoryLabels} 的分组标签对应。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/12
 */
public enum WorkShopWidgetCategory {

    CORE_DISPLAY("core-display"),
    VISUALIZATION("visualization"),
    FILTERING("filtering"),
    EVENT_NAVIGATION("event-navigation"),
    GROOVY_SCRIPT("groovy-script"),
    CUSTOM("custom");

    /** 前端 extractProps.category 使用的字符串值 */
    public final String val;

    WorkShopWidgetCategory(String val) {
        this.val = val;
    }
}