package com.qlangtech.tis.plugin.ontology.workshop.model.page;

import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.Validator;

/**
 * 上下布局模板：顶部 Header + 底部内容区，Header 高度可配置。
 *
 * <p>新增布局类型，原枚举没有对应项。适合以下场景：
 * <ul>
 *   <li><b>固定顶栏</b>：页面标题 + 全局操作按钮，下方是主内容区
 *   <li><b>通知条</b>：顶部重要提示 + 下方正常内容
 *   <li><b>筛选 + 列表</b>：顶部筛选器占固定高度，下方列表撑满剩余空间
 * </ul>
 *
 * <h3>配置示例</h3>
 * <pre>{@code
 * // 固定 80px 标题栏
 * headerHeight = new PixelHeaderHeight(80)
 *
 * // 固定 120px 筛选区
 * headerHeight = new PixelHeaderHeight(120)
 *
 * // 按内容自适应高度（auto）
 * headerHeight = new AutoHeaderHeight()
 * }</pre>
 *
 * <h3>CSS 输出</h3>
 * <p>前端按 {@code headerHeight} 的 impl 拼成 {@code grid-template-rows}：
 * <ul>
 *   <li>固定像素：{@code "120px minmax(0,1fr)"} —— 顶栏固定高度，内容区撑满剩余空间
 *   <li>自适应：{@code "auto minmax(0,1fr)"} —— 顶栏按内容高度，内容区撑满剩余空间
 * </ul>
 * 内容区恒为 {@code minmax(0,1fr)}，保证它始终撑满剩余垂直空间。
 *
 * <h3>注意事项</h3>
 * <ul>
 *   <li><b>固定像素是精确高度，超出即裁剪</b>：{@link PixelHeaderHeight} 会被前端拼成
 *       {@code grid-template-rows: 100px minmax(0,1fr)} 的硬行高，而单个分区的内容最小高度
 *       约 176px（标题栏 + 内边距 + 空投放区）。放不下的部分<b>不滚动、直接裁掉</b>
 *       （由 {@code workshop-page.component.less} 对首个栅格项开 {@code overflow: hidden}）。
 *       编辑态下投放区里的 Add widget / Set layout 按钮因此可能被裁 ——
 *       加 widget 走 section 标题栏右侧的「＋」。要「按内容长高」请选 {@link AutoHeaderHeight}。
 *   <li><b>不支持百分比</b>：CSS {@code grid-template-rows} 的百分比相对容器高度计算，
 *       而页面容器高度通常是 {@code auto}（由内容撑起），百分比会退化成 {@code auto}，
 *       无法达到"顶栏占 20% 高度"的效果。因此只提供固定像素和自适应两种单位。
 *   <li><b>窄屏不回落</b>：上下布局在手机上仍然合理（顶栏收窄、内容区滚动），
 *       不像左右布局需要强制单栏 —— 窄屏回落由前端
 *       {@code workshop-page.component.less} 的 768px 媒体查询统一处理。
 * </ul>
 *
 * @see PageTemplate
 * @see HeaderHeight
 */
public class HeaderContentPageTemplate extends PageTemplate {

    private static final long serialVersionUID = 1L;

    /**
     * Header 高度配置（多态插件）。
     *
     * <p>用户可以选择：
     * <ul>
     *   <li>{@link PixelHeaderHeight}：固定像素高度（如 80px），顶栏高度固定
     *   <li>{@link AutoHeaderHeight}：自适应高度（auto），顶栏高度由内容撑起
     * </ul>
     *
     * <p>缺省值 {@link PixelHeaderHeight}（80px），适合多数场景。
     */
    @FormField(ordinal = 1, validate = Validator.require)
    public HeaderHeight headerHeight;

    /**
     * XStream 反序列化后的补偿钩子，用于兜底旧数据或缺失字段。
     *
     * <p>如果 {@code headerHeight} 为 {@code null}（旧数据或配置丢失），
     * 兜底成 {@link PixelHeaderHeight}（80px）。
     *
     * @return 本实例（XStream 约定）
     */
    private Object readResolve() {
        if (headerHeight == null) {
            PixelHeaderHeight defaultHeight = new PixelHeaderHeight();
            defaultHeight.pixels = 80;
            headerHeight = defaultHeight;
        }
        return this;
    }

    @TISExtension
    public static class DefaultDescriptor extends PageTemplate.BaseDescriptor  {
        public String getDisplayName() {
            return "Header_Content_Page_Template";
        }
        @Override
        public EndType getEndType() {
            return EndType.LayoutHeaderContent;
        }
        @Override
        public String shortComment() {
            return "上下布局";
        }
    }
}
