package com.qlangtech.tis.plugin.ontology.workshop.model.page;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;

/**
 * 自适应头部高度配置：顶栏高度由内容撑起，不固定。
 *
 * <p>适合内容动态变化的场景，如可展开的通知条、动态加载的筛选器。
 *
 * <p><b>零字段插件</b>：本类没有任何配置字段，用户选择它即表示"让浏览器自动计算顶栏高度"。
 * 前端表单不会显示任何输入框。
 *
 * <p>与 {@link PixelHeaderHeight} 的行为差异：那个是<b>精确</b>高度、内容超出会被裁掉；
 * 本类产出的 {@code auto} 行由内容撑起，<b>不会被裁剪</b>（行高 = 内容高，没有可裁的部分）。
 *
 * <h3>前端如何消费</h3>
 * <p>本类只提供配置值；CSS 由前端 {@code page.model.ts} 的
 * {@code columnWidthToCss} / {@code headerHeightToCss} 按 impl 读 {@code vals} 拼出来 ——
 * 后端不再产 CSS 字符串（两处各算一份，单位换算迟早漂移）。
 *
 * @see HeaderHeight
 * @see HeaderContentPageTemplate
 */
public class AutoHeaderHeight extends HeaderHeight {

    private static final long serialVersionUID = 1L;


    @TISExtension
    public static class DefaultDescriptor extends HeaderHeight.BaseDescriptor implements DescriptorUseableShortComment {
        public String getDisplayName() {
            return "Auto_Header_Height";
        }

        @Override
        public String shortComment() {
            return "自动高度";
        }
    }
}