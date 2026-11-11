package com.qlangtech.tis.plugin.ontology.workshop.model.page;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;

/**
 * 固定像素头部高度配置：顶栏高度固定，不随内容变化。
 *
 * <p>适合已知内容高度的场景，如固定 80px 标题栏、固定 120px 筛选器区域。
 *
 * <h3>配置示例</h3>
 * <pre>{@code
 * // 固定 80px 标题栏
 * pixels = 80
 *
 * // 固定 120px 筛选区
 * pixels = 120
 * }</pre>
 *
 * <h3>前端如何消费</h3>
 * <p>本类只提供配置值；CSS 由前端 {@code page.model.ts} 的
 * {@code columnWidthToCss} / {@code headerHeightToCss} 按 impl 读 {@code vals} 拼出来 ——
 * 后端不再产 CSS 字符串（两处各算一份，单位换算迟早漂移）。
 *
 * @see HeaderHeight
 * @see HeaderContentPageTemplate
 */
public class PixelHeaderHeight extends HeaderHeight {

    private static final long serialVersionUID = 1L;

    /**
     * 像素值（40–400），**精确**高度。
     *
     * <p>前端把它拼进 {@code grid-template-rows} 的第一行。分区的内容最小高度约 176px
     * （标题栏 + 内边距 + 空投放区），超过本值的部分**不滚动、直接裁剪** ——
     * 这是「正好 100px」的语义，与 {@link AutoHeaderHeight}（由内容撑起）相反。
     *
     * <p>取值范围：40–400。过矮会把分区内容（含投放区里的 Add widget / Set layout 按钮）
     * 裁掉；过高会让下方内容区所剩无几。
     * 缺省值 80（常见顶栏高度）。
     */
    @FormField(ordinal = 1, type = FormFieldType.INT_NUMBER, validate = {Validator.require, Validator.integer})
    public Integer pixels = 80;


    @TISExtension
    public static class DefaultDescriptor extends HeaderHeight.BaseDescriptor implements DescriptorUseableShortComment {
        public String getDisplayName() {
            return "Pixel_Header_Height";
        }

        @Override
        public String shortComment() {
            return "像素高度";
        }

        /**
         * 校验顶栏像素高度取值范围（40–400）。
         *
         * <p>范围来自 {@link #pixels} 的 javadoc：过矮会把分区内容裁掉，过高会让内容区所剩无几。
         */
        public boolean validatePixels(IFieldErrorHandler msgHandler, Context context,
                                      String fieldName, String value) {
            int pixels = Integer.parseInt(value);
            if (pixels < 40 || pixels > 400) {
                msgHandler.addFieldError(context, fieldName,
                        "顶栏高度必须在 40 至 400 像素之间（过矮会把分区内容裁掉，过高会让下方内容区所剩无几）");
                return false;
            }
            return true;
        }
    }
}