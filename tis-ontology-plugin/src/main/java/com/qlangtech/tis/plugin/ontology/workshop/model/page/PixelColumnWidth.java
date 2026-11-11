package com.qlangtech.tis.plugin.ontology.workshop.model.page;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;

/**
 * 固定像素列宽配置：左栏宽度不随页面缩放。
 *
 * <p>适合"导航/列表 + 内容"场景，如详情布局（260px）、收件箱（340px），
 * 左栏宽度固定，右侧内容区自适应。
 *
 * <h3>配置示例</h3>
 * <pre>{@code
 * // 详情布局：固定 260px 导航栏
 * pixels = 260
 *
 * // 收件箱布局：固定 340px 列表
 * pixels = 340
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
public class PixelColumnWidth extends ColumnWidth {

    private static final long serialVersionUID = 1L;

    /**
     * 像素值（50–800），**精确**宽度。
     *
     * <p>前端把它拼进 {@code grid-template-columns} 的第一列。分区内宽度超出本值的
     * 内容**不滚动、直接裁剪**（由 {@code workshop-page.component.less} 对该栅格项开
     * {@code overflow: hidden}）—— 需要找回内容就调大本值。
     *
     * <p>取值范围：50–800（过窄放不下内容、且更容易裁到东西，过宽会在小屏上溢出）。
     * 缺省值 260（对应原 DETAILS 的默认宽度）。
     */
    @FormField(ordinal = 1, type = FormFieldType.INT_NUMBER, validate = {Validator.require, Validator.integer})
    public Integer pixels = 260;


    @TISExtension
    public static class DefaultDescriptor extends ColumnWidth.BaseDescriptor implements DescriptorUseableShortComment {
        public String getDisplayName() {
            return "Pixel_Column_Width";
        }

        @Override
        public String shortComment() {
            return "像素列宽";
        }

        /**
         * 校验左栏像素宽度取值范围（50–800）。
         *
         * <p>范围来自 {@link #pixels} 的 javadoc：过窄放不下内容，过宽会在小屏上溢出。
         */
        public boolean validatePixels(IFieldErrorHandler msgHandler, Context context,
                                      String fieldName, String value) {
            int pixels = Integer.parseInt(value);
            if (pixels < 50 || pixels > 800) {
                msgHandler.addFieldError(context, fieldName,
                        "左栏宽度必须在 50 至 800 像素之间（过窄放不下内容，过宽会在小屏上溢出）");
                return false;
            }
            return true;
        }
    }
}