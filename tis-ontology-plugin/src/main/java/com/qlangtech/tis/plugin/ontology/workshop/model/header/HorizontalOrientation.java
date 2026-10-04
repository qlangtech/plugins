package com.qlangtech.tis.plugin.ontology.workshop.model.header;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;

/**
 * 水平方向：header 是页面顶部的横向工具栏（Palantir 的默认形态），载荷是<b>高度</b>。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/20
 */
public class HorizontalOrientation extends HeaderOrientation {

    private static final long serialVersionUID = 1L;

    /**
     * 顶栏高度（px）。
     *
     * <p>刻意<b>不带</b> {@link Validator#require}：配置面板是「整对象提交」，
     * 用户清空数字框时会下发 null，加了 require 会让整次保存被拒 —— 只是想把高度
     * 暂时留空而已，不该拖累标题、颜色等其他字段的修改。
     */
    @FormField(ordinal = 0, type = FormFieldType.INT_NUMBER, validate = {Validator.integer})
    public Integer height = 64;

    @TISExtension
    public static class DefaultDescriptor extends BasicDescriptor implements DescriptorUseableShortComment {

        @Override
        public String getDisplayName() {
            return "Horizontal";
        }

        @Override
        public String shortComment() {
            return "水平页头";
        }
    }
}
