package com.qlangtech.tis.plugin.ontology.workshop.widget.impl;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.workshop.widget.WidgetTabConfig;
import com.qlangtech.tis.plugin.ontology.workshop.widget.FullDisplayWidget;
import com.qlangtech.tis.plugin.workshop.widget.IWorkshopWidget;

import java.util.List;

/**
 * P0 Widget：标签页（Tabs）
 * <p>
 * 标签页切换组件，支持 Header Tabs（切换 Page）和 Section Tabs（触发事件）两种模式。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/12
 */
public class TabsWidget extends FullDisplayWidget {

    public static final String KEY_TABS = "tabs";

    @FormField(type = FormFieldType.ENUM, ordinal = 10, advance = false)
    public TabStyle tabType = TabStyle.line;

    /**
     * 标签栏所处的位置。
     * <p>
     * 这里做成显式配置字段是<b>有意的取舍</b>：渲染器只给组件实例赋了 {@code widget}
     * （见前端 {@code widget-renderer.component.ts}），从不赋 {@code location}，
     * 所以组件上的 {@code @Input() location} 恒为 {@code undefined}，header 模式从不生效。
     * 渲染器本身并不知道自己身在 header 还是 section 内，正确修法是由布局容器透传；
     * 在那之前先用一个显式字段消除静默失效。
     * TODO 布局容器支持透传 location 后，改为自动推导并删除该字段。
     */
    @FormField(type = FormFieldType.ENUM, ordinal = 11, advance = false)
    public TabLocation location = TabLocation.section;

    @FormField(type = FormFieldType.MULTI_DESCRIBLE_PLUGIN, desClazz = WidgetTabConfig.class,
            ordinal = 12, advance = false, validate = {Validator.require})
    public List<WidgetTabConfig> tabs;

    /** 标签页视觉形态 */
    public enum TabStyle implements DescriptorUseableShortComment {
        line("线条风格"), card("卡片风格");
        public final String label;

        TabStyle(String label) {
            this.label = label;
        }

        @Override
        public String shortComment() {
            return this.label;
        }
    }

    /** 标签栏位置：Header 模式下切换 Page，Section 模式下触发事件 */
    public enum TabLocation implements DescriptorUseableShortComment {
        header("模块头部：切换 Page"), section("区块内：触发事件");
        public final String label;

        TabLocation(String label) {
            this.label = label;
        }

        @Override
        public String shortComment() {
            return this.label;
        }
    }

    @TISExtension
    public static class DescriptorImpl extends BaseWidgetDescriptor<IWorkshopWidget> {

        @Override
        public String getDisplayName() {
            return "Tabs";
        }

        @Override
        public WorkShopWidgetType getWidgetType() {
            return WorkShopWidgetType.TABS;
        }
    }
}
