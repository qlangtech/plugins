package com.qlangtech.tis.plugin.ontology.workshop.model.header;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.IEndTypeGetter;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.utils.InputColor;

/**
 * 「图标」形态的应用 Logo。
 *
 * <p>折叠态的联动见 {@link CollapsibleConfig#collapsedImage}：Palantir 的约定是
 * logo 选了图标时，折叠态直接复用该图标。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/11/14
 */
public class IconLogoConfig extends LogoConfig {

    private static final long serialVersionUID = 1L;

    /**
     * Ant Design 图标名（前端直接绑 {@code nzType}）。
     *
     * <p>声明方式与 {@code WidgetButtonConfig.leftIcon} 一致（EndType 枚举 +
     * {@code enumMode: model_assist} 的 json 条目）。
     */
    @FormField(ordinal = 0, type = FormFieldType.ENUM)
    public IEndTypeGetter.EndType icon;

    /** 图标颜色；留空 = 用主题默认 */
    @FormField(ordinal = 1, type = FormFieldType.Color)
    public InputColor iconColor;

    @TISExtension
    public static class DefaultDescriptor extends BasicDescriptor implements DescriptorUseableShortComment {

        @Override
        public String getDisplayName() {
            return "Icon";
        }

        @Override
        public String shortComment() {
            return "图标";
        }
    }
}
