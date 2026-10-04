package com.qlangtech.tis.plugin.ontology.workshop.model.header;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;

/**
 * 垂直方向：header 是页面左侧的纵向侧栏（Palantir 的可折叠导航栏形态），载荷是<b>宽度</b>。
 *
 * <p>注意「是否可折叠」不在本类上 —— 折叠是独立于方向的维度（水平顶栏同样可以折叠），
 * 因此它由 {@link com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopHeader#collapseConfig}
 * 单独承担，与本类平级。
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
