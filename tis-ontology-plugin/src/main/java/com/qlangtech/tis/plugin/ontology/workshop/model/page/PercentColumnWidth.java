package com.qlangtech.tis.plugin.ontology.workshop.model.page;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;

/**
 * 百分比列宽配置：左栏宽度按页面宽度的百分比计算。
 *
 * <p>适合"主副栏"场景，如空白页（30%）、等宽两栏（50%），左栏随页面缩放。
 *
 * <h3>配置示例</h3>
 * <pre>{@code
 * // 空白页：左侧占 30%
 * percentage = 30
 *
 * // 等宽两栏：左侧占 50%
 * percentage = 50
 * }</pre>
 *
 * <h3>前端如何消费</h3>
 * <p>本类只提供配置值；CSS 由前端 {@code page.model.ts} 的
 * {@code columnWidthToCss} / {@code headerHeightToCss} 按 impl 读 {@code vals} 拼出来 ——
 * 后端不再产 CSS 字符串（两处各算一份，单位换算迟早漂移）。
 *
 * @see ColumnWidth
 * @see TwoColumnPageTemplate
 */
public class PercentColumnWidth extends ColumnWidth {

    private static final long serialVersionUID = 1L;

    /**
     * 百分比值（1–99）。
     *
     * <p>取值范围：1–99（1% 以下左栏太窄放不下内容，100% 会让右栏消失）。
     * 缺省值 30（对应原 BLANK 的默认比例）。
     */
    @FormField(ordinal = 1, type = FormFieldType.INT_NUMBER, validate = {Validator.require, Validator.integer})
    public Integer percentage = 30;


    @TISExtension
    public static class DefaultDescriptor extends ColumnWidth.BaseDescriptor implements DescriptorUseableShortComment {
        public String getDisplayName() {
            return "Percent_Column_Width";
        }

        @Override
        public String shortComment() {
            return "百分比列宽";
        }

        /**
         * 校验左栏百分比取值范围（1–99）。
         *
         * <p>{@code Validator.require / Validator.integer} 只保证非空且为整数，
         * 范围本身是业务约束（见 {@link #percentage} 的 javadoc），必须在这里校验。
         */
        public boolean validatePercentage(IFieldErrorHandler msgHandler, Context context,
                                          String fieldName, String value) {
            int percentage = Integer.parseInt(value);
            if (percentage < 1 || percentage > 99) {
                msgHandler.addFieldError(context, fieldName,
                        "左栏百分比必须在 1 至 99 之间（1% 以下放不下内容，100% 会让右栏消失）");
                return false;
            }
            return true;
        }
    }
}