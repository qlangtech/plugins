package com.qlangtech.tis.plugin.ontology.workshop.model.header;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;

/**
 * 「图片」形态的应用 Logo。
 *
 * <p>对应 Palantir 的 "Select an image… Customize the image height. Position the image:
 * left/center/right for horizontal headers, top/bottom for vertical headers."
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/11/14
 */
public class ImageLogoConfig extends LogoConfig {

    private static final long serialVersionUID = 1L;

    public static final String KEY_POSITION = "position";

    /**
     * 图片地址
     */
    @FormField(ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.url})
    public String url;

    /**
     * 图片高度（px），宽度按比例缩放，前端 {@code object-fit: contain}
     */
    @FormField(ordinal = 1, type = FormFieldType.INT_NUMBER, validate = {Validator.require, Validator.integer})
    public Integer height = 24;

    /**
     * 图片在页头里的停靠位置：水平顶栏用 left/center/right，垂直侧栏用 top/bottom。
     * 取值固定（CLAUDE.md 原则二：固定取值用 ENUM），中文说明走
     * {@link LogoPosition#shortComment()}，不写 json enum。
     */
    @FormField(ordinal = 2, type = FormFieldType.ENUM)
    public LogoPosition position = LogoPosition.left;

    /**
     * 停靠位置；取值固定，硬编码于此
     */
    public enum LogoPosition implements DescriptorUseableShortComment {
        left("左侧（水平页头）"),
        center("居中（水平页头）"),
        right("右侧（水平页头）"),
        top("顶部（垂直侧栏）"),
        bottom("底部（垂直侧栏）");

        public final String label;

        LogoPosition(String label) {
            this.label = label;
        }

        @Override
        public String shortComment() {
            return this.label;
        }
    }

    @TISExtension
    public static class DefaultDescriptor extends BasicDescriptor implements DescriptorUseableShortComment {

        @Override
        public String getDisplayName() {
            return "Image";
        }

        @Override
        public String shortComment() {
            return "图片";
        }
    }
}
