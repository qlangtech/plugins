package com.qlangtech.tis.plugin.ontology.workshop.model.section;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.utils.InputColor;

import java.io.Serializable;

/**
 * Section 样式配置。
 *
 * <p>{@link #headerFormat} 已从枚举（NONE / TITLE / TITLE_WITH_DESCRIPTION）改为
 * 抽象 Describable 子类：描述字段只存在于 {@link TitleWithDescriptionHeaderFormat} 子类上，
 * 避免了「枚举声明了 TITLE_WITH_DESCRIPTION 却没有地方输入描述」的矛盾。
 */
public class SectionStyleConfig implements Describable<SectionStyleConfig>, Serializable {

    /**
     * 标题格式（多态）：{@link NoneHeaderFormat}（不显示）/
     * {@link TitleOnlyHeaderFormat}（仅标题）/
     * {@link TitleWithDescriptionHeaderFormat}（标题 + 描述）。
     *
     * <p>聚合属性字段<b>不能</b>声明 {@code type} —— 框架按字段类型实现
     * {@link Describable} 自动识别为嵌套子表单并生成 impl 选择器。
     */
    @FormField(ordinal = 0, validate = {Validator.require})
    public HeaderFormat headerFormat;

    /**
     * 背景颜色；留空 = 不改变。格式校验由 {@link FormFieldType#Color} 提供。
     */
    @FormField(ordinal = 1, type = FormFieldType.Color)
    public InputColor backgroundColor;

    @FormField(ordinal = 2, type = FormFieldType.ENUM)
    public BorderStyle borderStyle = BorderStyle.NONE;

    @FormField(ordinal = 3, type = FormFieldType.INT_NUMBER)
    public Integer padding = 16;

    @TISExtension
    public static class DefaultDescriptor extends Descriptor<SectionStyleConfig> {
        @Override
        public String getDisplayName() {
            return "Section Style Config";
        }
    }

    public enum BorderStyle implements DescriptorUseableShortComment {
        NONE("无边框"),
        SOLID("实线"),
        DASHED("虚线"),
        ROUNDED("圆角");

        private final String comment;

        BorderStyle(String comment) {
            this.comment = comment;
        }

        @Override
        public String shortComment() {
            return comment;
        }
    }
}