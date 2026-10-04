package com.qlangtech.tis.plugin.ontology.workshop.model.header;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;

/**
 * 「可折叠」形态的 header 折叠配置。
 *
 * <p>开关由子类类型承担，故本类没有任何 {@code collapsible} 字段 —— 能构造出本类的
 * 实例，就意味着 header 可折叠。对照 {@link NoneCollapseConfig}。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/20
 */
public class CollapsibleConfig extends CollapseConfig {

    private static final long serialVersionUID = 1L;

    public static final String KEY_COLLAPSED_BY_DEFAULT = "collapsedByDefault";
    public static final String KEY_COLLAPSED_IMAGE = "collapsedImage";

    /**
     * 打开模块时是否默认处于折叠态（Palantir 的 "Start collapsed"）。
     */
    @FormField(ordinal = 0, type = FormFieldType.ENUM)
    public Boolean collapsedByDefault = false;

    /**
     * 折叠态下代替标题显示的图标 URL。
     *
     * <p>非必填：不填时前端退化为只显示折叠按钮，是合法形态。
     */
    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT)
    public String collapsedImage;

    @TISExtension
    public static class DefaultDescriptor extends BasicDescriptor implements DescriptorUseableShortComment {

        @Override
        public String getDisplayName() {
            return "Collapsible";
        }

        @Override
        public String shortComment() {
            return "可折叠侧栏";
        }
    }
}
