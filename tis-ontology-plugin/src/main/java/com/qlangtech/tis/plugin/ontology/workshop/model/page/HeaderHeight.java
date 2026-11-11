package com.qlangtech.tis.plugin.ontology.workshop.model.page;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;

import java.io.Serializable;

/**
 * 头部高度配置抽象基类，定义页面上下布局中顶栏的高度。
 *
 * <p>取代原先"枚举 + 整数值"的耦合设计（{@code HeightUnit headerHeightUnit + Integer headerHeight}），
 * 改为多态插件：每个单位对应一个派生类，高度值作为派生类的字段。
 *
 * <h3>派生类</h3>
 * <ul>
 *   <li>{@link PixelHeaderHeight}：固定像素高度（如 80px），顶栏高度固定
 *   <li>{@link AutoHeaderHeight}：自适应高度（auto），顶栏高度由内容撑起
 * </ul>
 *
 * <h3>为什么需要多态而非枚举</h3>
 * <p>原设计的问题：
 * <pre>{@code
 * public Integer headerHeight;           // 高度值
 * public HeightUnit headerHeightUnit;    // 单位（PX / AUTO）
 * }</pre>
 * 这两个字段是强耦合的：{@code headerHeightUnit} 决定了 {@code headerHeight} 是否生效
 * （PX 时生效，AUTO 时忽略）。这种设计违背了 TIS 插件的多态原则。
 *
 * <p>多态设计的优势：
 * <ol>
 *   <li><b>类型安全</b>：{@link PixelHeaderHeight} 有 {@code pixels} 字段，
 *       {@link AutoHeaderHeight} 没有字段（零配置）
 *   <li><b>扩展性</b>：未来可以无缝添加新单位（如 {@code vh}、{@code em}），
 *       不用改 {@code HeaderContentPageTemplate} 的字段
 *   <li><b>表单体验</b>：前端自动按选中的派生类显示对应的配置字段，
 *       选固定像素显示输入框，选自适应不显示任何输入框
 * </ol>
 *
 * <h3>为什么不支持百分比</h3>
 * <p>CSS {@code grid-template-rows} 的百分比相对容器高度计算，
 * 而页面容器高度通常是 {@code auto}（由内容撑起），百分比会退化成 {@code auto}，
 * 无法达到"顶栏占 20% 高度"的效果。因此只提供固定像素和自适应两种单位。
 *
 * <h3>序列化格式</h3>
 * <p>标准 TIS 插件形状 {@code {impl, vals}}，{@code impl} 是具体插件类的<b>全限定名</b>
 * （{@link Descriptor#getId()} 恒等于 {@code clazz.getName()}）：
 * <pre>{@code
 * // 固定像素
 * {
 *   "impl": "com.qlangtech.tis.plugin.ontology.workshop.model.page.PixelHeaderHeight",
 *   "vals": { "pixels": 80 }
 * }
 *
 * // 自适应（零字段）
 * {
 *   "impl": "com.qlangtech.tis.plugin.ontology.workshop.model.page.AutoHeaderHeight",
 *   "vals": {}
 * }
 * }</pre>
 *
 * <h3>CSS 由前端算</h3>
 * <p>本层不产出 CSS 字符串 —— 前端 {@code page.model.ts#headerHeightToCss} 按 impl 读
 * {@code vals} 拼出 {@code grid-template-rows} 的顶栏值。
 *
 * @see HeaderContentPageTemplate#headerHeight
 * @see PixelHeaderHeight
 * @see AutoHeaderHeight
 */
public abstract class HeaderHeight implements Describable<HeaderHeight>, Serializable {

    private static final long serialVersionUID = 1L;

    // 刻意不提供「转 CSS 字符串」的方法（早先有过一个 toCssValue()，已删）：CSS 由前端
    // page.model.ts#headerHeightToCss 按 impl 读 vals 拼出来。后端再产一份，单位换算
    // 就有两个来源，迟早漂移。
    //
    // 刻意不覆写 getDescriptor()：RootFormProperties 会把它直接传给 DescribableJSON，
    // 返回 null 会触发 NPE。用 Describable 的默认实现
    // （TIS.get().getDescriptor(this.getClass())），按具体子类的 class 查 @TISExtension。

    public abstract static class BaseDescriptor extends Descriptor<HeaderHeight> {
        // 子类 Descriptor 可以在这里添加通用方法
    }
}