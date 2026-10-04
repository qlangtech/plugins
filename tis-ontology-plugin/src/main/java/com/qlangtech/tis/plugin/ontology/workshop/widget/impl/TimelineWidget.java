package com.qlangtech.tis.plugin.ontology.workshop.widget.impl;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.ontology.workshop.enums.VariableType;
import com.qlangtech.tis.plugin.ontology.workshop.widget.FullDisplayWidget;
import com.qlangtech.tis.plugin.workshop.widget.IWorkshopWidget;

/**
 * P0 Widget：时间线（Timeline）
 * <p>
 * 以时间线形式展示事件序列，支持自定义颜色、图标、时间戳和内容。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/12
 */
public class TimelineWidget extends FullDisplayWidget {

    public static final String KEY_DATA_SOURCE_VAR = "dataSourceVar";

    /** 事件来源：当前模块中的对象集合或时间序列集合变量（options 由 Descriptor 动态供给） */
    @FormField(type = FormFieldType.SELECTABLE, ordinal = 10, advance = false)
    public String dataSourceVar;

    /** 时间线节点相对轴线的位置 */
    @FormField(type = FormFieldType.ENUM, ordinal = 11, advance = false)
    public TimelineMode mode = TimelineMode.left;

    /**
     * 条目内部的颜色 / 图标 / 时间戳 / 正文都取自数据本身（前端读 {@code item.color} 等），
     * 不对应配置字段。
     */
    public enum TimelineMode implements DescriptorUseableShortComment {
        left("左侧"), right("右侧"), alternate("交替");
        public final String label;

        TimelineMode(String label) {
            this.label = label;
        }

        @Override
        public String shortComment() {
            return this.label;
        }
    }

    @TISExtension
    public static class DescriptorImpl extends BaseWidgetDescriptor<IWorkshopWidget> {

        public DescriptorImpl() {
            super();
            // 同时接受两种类型：时间线的数据既可能是带时间属性的对象集合，也可能是时间序列集合。
            // 只认后者会让下拉在只建了对象集合的模块里恒为空。
            this.registerSelectOptions(KEY_DATA_SOURCE_VAR,
                    () -> WidgetOptionHelper.getVariableOptions(
                            VariableType.OBJECT_SET, VariableType.TIME_SERIES_SET));
        }

        @Override
        public String getDisplayName() {
            return "Timeline";
        }

        @Override
        public WorkShopWidgetType getWidgetType() {
            return WorkShopWidgetType.TIMELINE;
        }
    }
}