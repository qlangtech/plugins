package com.qlangtech.tis.plugin.ontology.workshop.widget.impl;

import com.qlangtech.tis.plugin.IEndTypeGetter.EndType;

/**
 * Workshop Widget 类型枚举 —— 描述每个 Widget 的前端元数据
 * <p>
 * 四个属性：
 * <ul>
 *   <li>{@code icon} —— 前端图标，取 {@link EndType}（其 {@code val} 即图标名）</li>
 *   <li>{@code category} —— 调色板分组类别（core-display / visualization / filtering / event-navigation）</li>
 *   <li>{@code widgetType} —— 前端 {@code renderRegistry} 注册用的短 key（例如 "date-time-picker"）</li>
 *   <li>{@code fullDisplayWidget} —— 该 Widget 用的是「完整显示配置」还是「精简显示配置」，
 *       见下文</li>
 * </ul>
 *
 * <h3>fullDisplayWidget 的口径</h3>
 * 这是<b>分类事实</b>，不是可配置项：它决定该 Widget 的 Display 表单是
 * {@code WidgetDisplayConfig}（宽度 / 高度 / 展示优化）还是 {@code CompactDisplayConfig}
 * （只有宽度）。历史来源是已删除的两个中间基类 {@code FullDisplayWidget} /
 * {@code CompactDisplayWidget}，取值必须与之逐项一致（{@code TestWidgetVariableContract}
 * 有一条断言把这里与 {@code AbstractWidgetDisplayConfig.configClazzOf} 钉在一起）。
 * <p>
 * 判据：<b>是否有独立的内容区</b>。表格 / 图表 / 看板卡片 / Markdown 正文这类有，
 * 单行表单控件（输入框、日期选择器、复选框）与页头文字、按钮组没有。
 * <p>
 * 由 {@link BaseWidgetDescriptor#getExtractProps()} final 方法统一消费，
 * 各 Widget Descriptor 需实现 {@code getWidgetType()} 返回对应枚举常量。
 * <p>
 * <b>icon 为什么是 EndType 而不是裸字符串：</b>图标名从此由 {@link EndType} 声明一次 ——
 * 写错名字编译期就挂，不会再出现「字符串拼错、前端静默渲染成空」这类失败。
 * 选用的常量见 {@link BaseWidgetDescriptor#getWidgetType()} 各实现处，
 * 新图标在 {@code IEndTypeGetter.EndType} 里按 {@code EndTypeCategory.Icon} 声明。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/12
 */
public enum WorkShopWidgetType {

    // ==================================================================
    //  Core Display Widgets（5 个）
    // ==================================================================
    OBJECT_TABLE(EndType.Table, WorkShopWidgetCategory.CORE_DISPLAY, "object-table", true),
    OBJECT_LIST(EndType.UnorderedList, WorkShopWidgetCategory.CORE_DISPLAY, "object-list", true),
    OBJECT_VIEW(EndType.Eye, WorkShopWidgetCategory.CORE_DISPLAY, "object-view", true),
    PROPERTY_LIST(EndType.Profile, WorkShopWidgetCategory.CORE_DISPLAY, "property-list", true),
    HEADER_TEXT(EndType.FontSize, WorkShopWidgetCategory.CORE_DISPLAY, "header-text", false),

    // ==================================================================
    //  Visualization Widgets（9 个）
    // ==================================================================
    CHART_XY(EndType.LineChart, WorkShopWidgetCategory.VISUALIZATION, "chart-xy", true),
    CHART_PIE(EndType.PieChart, WorkShopWidgetCategory.VISUALIZATION, "chart-pie", true),
    METRIC_CARD(EndType.Dashboard, WorkShopWidgetCategory.VISUALIZATION, "metric-card", true),
    TIMELINE(EndType.FieldTime, WorkShopWidgetCategory.VISUALIZATION, "timeline", true),
    // 未实现。启用时**不能**照抄下面的 EndType.Blank —— "blank" 不是合法的 Ant Design
    // 图标名，页面不会报错，调色板里这张卡片只是没有图标。先挑一个真实存在的图标名。
   // MAP(EndType.Blank, WorkShopWidgetCategory.VISUALIZATION, "map"),
    PIVOT_TABLE(EndType.PivotTable, WorkShopWidgetCategory.VISUALIZATION, "pivot-table", true),
    GANTT_CHART(EndType.Gantt, WorkShopWidgetCategory.VISUALIZATION, "gantt-chart", true),
    MARKDOWN(EndType.FileMarkdown, WorkShopWidgetCategory.CORE_DISPLAY, "markdown", true),

    // ==================================================================
    //  Filtering Widgets（6 个）
    // ==================================================================
    FILTER_LIST(EndType.Filter, WorkShopWidgetCategory.FILTERING, "filter-list", true),
    OBJECT_DROPDOWN(EndType.DownSquare, WorkShopWidgetCategory.FILTERING, "object-dropdown", false),
    TEXT_INPUT(EndType.Form, WorkShopWidgetCategory.FILTERING, "text-input", false),
    DATE_TIME_PICKER(EndType.Calendar, WorkShopWidgetCategory.FILTERING, "date-time-picker", false),
    CHECKBOX(EndType.CheckSquare, WorkShopWidgetCategory.FILTERING, "checkbox", false),
    /**
     * 范围日期选择；单值归 {@link #DATE_TIME_PICKER}，两者职责互斥。
     * <p>
     * 与 {@link #DATE_TIME_PICKER} 共用同一个 {@link EndType#Calendar} —— 两个 Widget 当前
     * 用的就是同一个前端图标，共用常量即如实反映这一点；将来要区分时在这里换一个 EndType。
     */
    DATE_INPUT(EndType.Calendar, WorkShopWidgetCategory.FILTERING, "date-input", false),

    // ==================================================================
    //  Event-triggering & Navigational Widgets（2 个）
    // ==================================================================
    BUTTON_GROUP(EndType.Block, WorkShopWidgetCategory.EVENT_NAVIGATION, "button-group", false),
    TABS(EndType.Bars, WorkShopWidgetCategory.EVENT_NAVIGATION, "tabs", true);

    /**
     * 前端图标。取值是 {@link EndType}，其 {@code val} 即前端 {@code nz-icon} 认的图标名 ——
     * 下发时由 {@link BaseWidgetDescriptor#getExtractProps()} 取 {@code icon.getVal()}。
     * <p>
     * ⚠️ {@code Table} / {@code PivotTable} / {@code Gantt} 这三条的 {@code containICON=true}
     * （TIS 自绘 svg），其余一律 {@code false}（图标名同时是 Ant Design 图标名，
     * 前端从 assets 按名动态加载）。取舍与新增步骤见 {@code IEndTypeGetter.EndType} 里那段注释。
     */
    public final EndType icon;

    /** 调色板分组类别 */
    public final WorkShopWidgetCategory category;

    /** 前端 renderRegistry 注册短 key */
    public final String widgetType;

    /**
     * 该 Widget 是否用「完整显示配置」（{@code WidgetDisplayConfig}）；{@code false} 则用
     * 「精简显示配置」（{@code CompactDisplayConfig}，只有宽度）。
     * <p>
     * 由 {@link BaseWidgetDescriptor#getExtractProps(boolean)} 下发为 {@code extractProps.fullDisplay}，
     * 前端据此选 Display 表单；后端由
     * {@code AbstractWidgetDisplayConfig.configClazzOf} 消费，落盘时就按它决定 impl。
     * 口径见类注释。
     */
    public final boolean fullDisplayWidget;

    WorkShopWidgetType(EndType icon, WorkShopWidgetCategory category, String widgetType, boolean fullDisplayWidget) {
        this.icon = icon;
        this.category = category;
        this.widgetType = widgetType;
        this.fullDisplayWidget = fullDisplayWidget;
    }
}