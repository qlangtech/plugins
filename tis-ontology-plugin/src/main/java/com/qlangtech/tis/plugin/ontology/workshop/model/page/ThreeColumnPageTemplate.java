package com.qlangtech.tis.plugin.ontology.workshop.model.page;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;

/**
 * 三栏布局模板：三段式等宽分栏。
 *
 * <p>对应原枚举的 THREE_COLUMN，无特定业务语义的纯栅格。
 * 典型场景是 inbox 的细分形态：筛选栏 | 列表 | 详情。
 *
 * <h3>使用场景</h3>
 * <ul>
 *   <li><b>分诊工作台细分</b>：左侧筛选器、中间列表、右侧详情/操作
 *   <li><b>三段式对比</b>：并排对比三个对象、三个时间段的数据
 *   <li><b>多级导航</b>：一级分类 | 二级分类 | 内容区
 * </ul>
 *
 * <h3>CSS 输出</h3>
 * <p>三栏等宽：
 * <pre>{@code
 * grid-template-columns: repeat(3, minmax(0, 1fr));
 * }</pre>
 * 即每栏占 1/3 宽度，且最小宽度为 0（允许内容收缩/截断，避免撑爆容器）。
 *
 * <h3>注意事项</h3>
 * <ul>
 *   <li><b>窄屏回落单栏</b>：三栏在手机上会挤成不可读的宽度，
 *       因此 ≤768px 时由前端 {@code workshop-page.component.less} 强制单栏
 *   <li><b>建页默认铺 3 个 Section</b>：分区 1 | 分区 2 | 分区 3
 * </ul>
 *
 * @see PageTemplate
 * @see TwoColumnPageTemplate
 */
public class ThreeColumnPageTemplate extends PageTemplate {

    private static final long serialVersionUID = 1L;

    @TISExtension
    public static class DefaultDescriptor extends PageTemplate.BaseDescriptor implements DescriptorUseableShortComment {
        public String getDisplayName() {
            return "Three_Column_Page_Template";
        }
        @Override
        public EndType getEndType() {
            return EndType.LayoutThreeColumn;
        }
        @Override
        public String shortComment() {
            return "三栏布局";
        }
    }
}
