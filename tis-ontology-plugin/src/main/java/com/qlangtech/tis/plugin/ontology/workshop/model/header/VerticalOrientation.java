package com.qlangtech.tis.plugin.ontology.workshop.model.header;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;

/**
 * 垂直方向：header 是页面左侧的纵向侧栏（Palantir 的可折叠导航栏形态），载荷是<b>宽度</b>
 * 与<b>折叠配置</b>。
 *
 * <p>「是否可折叠」挂在<b>本类</b>而非
 * {@link com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopHeader} 顶层：折叠语义只在
 * 垂直侧栏下成立 —— 前端的折叠视觉（宽度收窄成 64px、折叠图标、内容隐藏）全部只对 vertical
 * 实现，水平顶栏配上折叠只会渲染出崩坏的排版；Palantir 同样把折叠设置门控在垂直方向之下
 * （concepts-layouts.md："Once you enable the vertical orientation, several additional
 * settings will become available: … Enable collapsibility…"）。曾经本字段挂在顶层，
 * 「水平 + 可折叠」这种自相矛盾的状态在类型层面可表示，实测渲染就是坏的。
 *
 * <p>折叠配置本身仍是 off/on 多态（{@link #collapseConfig} 取
 * {@link NoneCollapseConfig} / {@link CollapsibleConfig}），由子类类型承担，
 * 避免出现「不可折叠却带着折叠图标」的字段袋。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/20
 */
public class VerticalOrientation extends HeaderOrientation {

    private static final long serialVersionUID = 1L;

    /**
     * 侧栏宽度（px）。与 {@link HorizontalOrientation#height} 同理不加
     * {@link Validator#require}。
     */
    @FormField(ordinal = 0, type = FormFieldType.INT_NUMBER, validate = {Validator.integer})
    public Integer width = 200;

    /**
     * 折叠配置：不可折叠 / 可折叠（可折叠时才有默认折叠与折叠图标）。
     *
     * <p>初始化表达式只覆盖 {@code new} 路径；XStream 反序列化不走构造器，
     * 老 XML 里没有本元素时字段为 null，由 {@link #readResolve()} 兜底补默认值。
     */
    @FormField(ordinal = 1, validate = {Validator.require})
    public CollapseConfig collapseConfig = new NoneCollapseConfig();

    /**
     * XStream 反序列化兜底：老 XML（collapseConfig 尚未迁入本类、或迁移初期）没有
     * {@code <collapseConfig>} 元素时补上「不可折叠」，否则前端配置面板的下拉是空的。
     */
    private Object readResolve() {
        if (this.collapseConfig == null) {
            this.collapseConfig = new NoneCollapseConfig();
        }
        return this;
    }

    @TISExtension
    public static class DefaultDescriptor extends BasicDescriptor implements DescriptorUseableShortComment {

        @Override
        public String getDisplayName() {
            return "Vertical";
        }

        @Override
        public String shortComment() {
            return "垂直侧栏";
        }
    }
}
