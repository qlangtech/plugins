package com.qlangtech.tis.plugin.ontology.workshop.widget.impl;

/**
 * Workshop Widget 类型枚举 —— 描述每个 Widget 的前端元数据
 * <p>
 * 三个属性：
 * <ul>
 *   <li>{@code icon} —— 前端 Ant Design 图标名</li>
 *   <li>{@code category} —— 调色板分组类别（core-display / visualization / filtering / event-navigation）</li>
 *   <li>{@code widgetType} —— 前端 {@code renderRegistry} 注册用的短 key（例如 "date-time-picker"）</li>
 * </ul>
 * <p>
 * 由 {@link BaseWidgetDescriptor#getExtractProps()} final 方法统一消费，
 * 各 Widget Descriptor 需实现 {@code getWidgetType()} 返回对应枚举常量。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/12
 */
public enum WorkShopWidgetType {

    // ==================================================================
    //  Core Display Widgets（5 个）
    // ==================================================================
    OBJECT_TABLE("table", WorkShopWidgetCategory.CORE_DISPLAY, "object-table"),
    OBJECT_LIST("unordered-list", WorkShopWidgetCategory.CORE_DISPLAY, "object-list"),
    OBJECT_VIEW("eye", WorkShopWidgetCategory.CORE_DISPLAY, "object-view"),
    PROPERTY_LIST("profile", WorkShopWidgetCategory.CORE_DISPLAY, "property-list"),
    HEADER_TEXT("font-size", WorkShopWidgetCategory.CORE_DISPLAY, "header-text"),

    // ==================================================================
    //  Visualization Widgets（9 个）
    // ==================================================================
    CHART_XY("line-chart", WorkShopWidgetCategory.VISUALIZATION, "chart-xy"),
    CHART_PIE("pie-chart", WorkShopWidgetCategory.VISUALIZATION, "chart-pie"),
    METRIC_CARD("dashboard", WorkShopWidgetCategory.VISUALIZATION, "metric-card"),
    TIMELINE("field-time", WorkShopWidgetCategory.VISUALIZATION, "timeline"),
   // MAP("environment", WorkShopWidgetCategory.VISUALIZATION, "map"),
    PIVOT_TABLE("pivot-table", WorkShopWidgetCategory.VISUALIZATION, "pivot-table"),
    GANTT_CHART("gantt", WorkShopWidgetCategory.VISUALIZATION, "gantt-chart"),
    MARKDOWN("file-markdown", WorkShopWidgetCategory.CORE_DISPLAY, "markdown"),

    // ==================================================================
    //  Filtering Widgets（5 个）
    // ==================================================================
    FILTER_LIST("filter", WorkShopWidgetCategory.FILTERING, "filter-list"),
    OBJECT_DROPDOWN("down-square", WorkShopWidgetCategory.FILTERING, "object-dropdown"),
    TEXT_INPUT("form", WorkShopWidgetCategory.FILTERING, "text-input"),
    DATE_TIME_PICKER("calendar", WorkShopWidgetCategory.FILTERING, "date-time-picker"),
    CHECKBOX("check-square", WorkShopWidgetCategory.FILTERING, "checkbox"),

    // ==================================================================
    //  Event-triggering & Navigational Widgets（3 个）
    // ==================================================================
    BUTTON_GROUP("block", WorkShopWidgetCategory.EVENT_NAVIGATION, "button-group"),
    TABS("bars", WorkShopWidgetCategory.EVENT_NAVIGATION, "tabs"),
    ACTION_BUTTON("play-square", WorkShopWidgetCategory.EVENT_NAVIGATION, "action-button");

    /** 前端 Ant Design 图标名 */
    public final String icon;

    /** 调色板分组类别 */
    public final WorkShopWidgetCategory category;

    /** 前端 renderRegistry 注册短 key */
    public final String widgetType;

    WorkShopWidgetType(String icon, WorkShopWidgetCategory category, String widgetType) {
        this.icon = icon;
        this.category = category;
        this.widgetType = widgetType;
    }
}