package com.qlangtech.tis.plugin.ontology.workshop.model.page;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.Validator;

/**
 * 两栏布局模板：左右分栏，左侧宽度可配置。
 *
 * <p>合并了原枚举的 BLANK / DETAILS / INBOX / TWO_COLUMN 四个变体 —— 它们的唯一区别就是左栏宽度：
 * <ul>
 *   <li>BLANK：30%（实测自 Foundry 截图）
 *   <li>DETAILS：260px（固定宽导航栏）
 *   <li>INBOX：340px（列表栏比导航栏宽一档，要放标题与摘要）
 *   <li>TWO_COLUMN：50%（等宽）
 * </ul>
 * 现在统一成一个模板 + 一个多态配置字段（{@link ColumnWidth}），用户可以选择百分比或像素。
 *
 * <h3>使用场景</h3>
 * <ul>
 *   <li><b>详情布局</b>（DETAILS）：左侧 260px 导航栏 + 右侧自适应内容区，
 *       适合以单个对象为中心的页面（顶部对象属性、中部图表、底部关联对象列表）
 *   <li><b>收件箱布局</b>（INBOX）：左侧 340px 列表栏 + 右侧详情/操作区，
 *       适合分诊工作台（左侧队列、右侧处置）
 *   <li><b>空白页</b>（BLANK）：左侧 30% + 右侧自适应，Foundry 新建页的默认态
 *   <li><b>等宽两栏</b>（TWO_COLUMN）：左侧 50% + 右侧 50%，无特定业务语义的纯栅格
 * </ul>
 *
 * <h3>配置示例</h3>
 * <pre>{@code
 * // 详情布局：固定 260px 导航栏
 * leftColumnWidth = new PixelColumnWidth(260)
 *
 * // 收件箱布局：固定 340px 列表
 * leftColumnWidth = new PixelColumnWidth(340)
 *
 * // 空白页：左侧占 30%
 * leftColumnWidth = new PercentColumnWidth(30)
 *
 * // 等宽两栏：左侧占 50%
 * leftColumnWidth = new PercentColumnWidth(50)
 * }</pre>
 *
 * <h3>CSS 输出</h3>
 * <p>前端按 {@code leftColumnWidth} 的 impl 拼成 {@code grid-template-columns}：
 * <ul>
 *   <li>百分比：{@code "30% minmax(0,1fr)"} —— 左栏固定比例，右栏自适应
 *   <li>像素：{@code "260px minmax(0,1fr)"} —— 左栏固定宽度，右栏自适应
 * </ul>
 * 右栏恒为 {@code minmax(0,1fr)}，即"最小 0、最大撑满剩余空间"，这样左栏宽度变化时右栏会自动调整。
 *
 * <h3>注意事项</h3>
 * <ul>
 *   <li><b>左栏是硬轨道，超出即裁剪</b>：无论百分比还是像素，左栏轨道宽度是定死的，而分区里
 *       装的内容不会自己缩（表格、长串、宽图都有很大的 min-content 宽度）。
 *       放不下的部分<b>不滚动、直接裁掉</b>（由 {@code workshop-page.component.less} 对
 *       {@code .page-template-two-column} 的首个栅格项开 {@code overflow: hidden}）。
 *       发现内容被裁，把左栏调宽即可找回；给像素模式一个较小的值是这一代价的主要来源。
 *   <li><b>窄屏回落单栏</b>：768px 以下由前端媒体查询统一改成
 *       {@code minmax(0,1fr)} 单栏 —— 多栏在手机上会挤成读不了的宽度。
 * </ul>
 *
 * @see PageTemplate
 * @see ColumnWidth
 */
public class TwoColumnPageTemplate extends PageTemplate {

    private static final long serialVersionUID = 1L;

    /**
     * 左栏宽度配置（多态插件）。
     *
     * <p>用户可以选择：
     * <ul>
     *   <li>{@link PercentColumnWidth}：百分比列宽（如 30%），左栏随页面缩放
     *   <li>{@link PixelColumnWidth}：固定像素列宽（如 260px），左栏不随页面缩放
     * </ul>
     *
     * <p>缺省值 {@link PercentColumnWidth}（30%），对应原 BLANK 的默认比例。
     */
    @FormField(ordinal = 1, validate = Validator.require)
    public ColumnWidth leftColumnWidth;

    /**
     * XStream 反序列化后的补偿钩子，用于兜底旧数据或缺失字段。
     *
     * <p>如果 {@code leftColumnWidth} 为 {@code null}（旧数据或配置丢失），
     * 兜底成 {@link PercentColumnWidth}（30%）。
     *
     * @return 本实例（XStream 约定）
     */
    private Object readResolve() {
        if (leftColumnWidth == null) {
            PercentColumnWidth defaultWidth = new PercentColumnWidth();
            defaultWidth.percentage = 30;
            leftColumnWidth = defaultWidth;
        }
        return this;
    }

    @TISExtension
    public static class DefaultDescriptor extends PageTemplate.BaseDescriptor implements DescriptorUseableShortComment {
        public String getDisplayName() {
            return "Two_Column_Page_Template";
        }

        @Override
        public EndType getEndType() {
            return EndType.LayoutTwoColumn;
        }

        @Override
        public String shortComment() {
            return "两栏布局";
        }
    }
}
