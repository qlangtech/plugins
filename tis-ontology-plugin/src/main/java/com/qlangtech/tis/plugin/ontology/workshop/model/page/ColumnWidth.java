package com.qlangtech.tis.plugin.ontology.workshop.model.page;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;

import java.io.Serializable;

/**
 * 列宽配置抽象基类，定义页面两栏布局中左栏的宽度。
 *
 * <p>取代原先"枚举 + 整数值"的耦合设计（{@code WidthUnit leftColumnUnit + Integer leftColumnWidth}），
 * 改为多态插件：每个单位对应一个派生类，宽度值作为派生类的字段。
 *
 * <h3>派生类</h3>
 * <ul>
 *   <li>{@link PercentColumnWidth}：百分比列宽（如 30%），左栏随页面缩放
 *   <li>{@link PixelColumnWidth}：固定像素列宽（如 260px），左栏不随页面缩放
 * </ul>
 *
 * <h3>为什么需要多态而非枚举</h3>
 * <p>原设计的问题：
 * <pre>{@code
 * public Integer leftColumnWidth;        // 宽度值
 * public WidthUnit leftColumnUnit;       // 单位（PERCENT / PX）
 * }</pre>
 * 这两个字段是强耦合的：{@code leftColumnUnit} 决定了 {@code leftColumnWidth} 的语义
 * （百分比时范围 1–99，像素时范围 50–800）。这种设计违背了 TIS 插件的多态原则。
 *
 * <p>多态设计的优势：
 * <ol>
 *   <li><b>类型安全</b>：每个派生类的字段有独立的校验规则（百分比 1–99，像素 50–800）
 *   <li><b>扩展性</b>：未来可以无缝添加新单位（如 {@code em}、{@code vw}），
 *       不用改 {@code TwoColumnPageTemplate} 的字段
 *   <li><b>表单体验</b>：前端自动按选中的派生类显示对应的配置字段，
 *       选百分比只显示百分比输入框，选像素只显示像素输入框
 * </ol>
 *
 * <h3>序列化格式</h3>
 * <p>标准 TIS 插件形状 {@code {impl, vals}}，{@code impl} 是具体插件类的<b>全限定名</b>
 * （{@link Descriptor#getId()} 恒等于 {@code clazz.getName()}）：
 * <pre>{@code
 * // 百分比
 * {
 *   "impl": "com.qlangtech.tis.plugin.ontology.workshop.model.page.PercentColumnWidth",
 *   "vals": { "percentage": 30 }
 * }
 *
 * // 像素
 * {
 *   "impl": "com.qlangtech.tis.plugin.ontology.workshop.model.page.PixelColumnWidth",
 *   "vals": { "pixels": 260 }
 * }
 * }</pre>
 *
 * <h3>CSS 由前端算</h3>
 * <p>本层不产出 CSS 字符串 —— 前端 {@code page.model.ts#columnWidthToCss} 按 impl 读
 * {@code vals} 拼出 {@code grid-template-columns} 的左栏值。后端如果也产一份，
 * 单位换算就有两个来源，迟早漂移。
 *
 * @see TwoColumnPageTemplate#leftColumnWidth
 * @see PercentColumnWidth
 * @see PixelColumnWidth
 */
public abstract class ColumnWidth implements Describable<ColumnWidth>, Serializable {

    private static final long serialVersionUID = 1L;

    // 刻意不提供「转 CSS 字符串」的方法：CSS 由前端 page.model.ts#columnWidthToCss 按
    // impl 读 vals 拼出来。后端再产一份，单位换算就有两个来源。

    // 刻意不覆写 getDescriptor()：RootFormProperties 会把它直接传给 DescribableJSON，
    // 返回 null 会触发 NPE。用 Describable 的默认实现
    // （TIS.get().getDescriptor(this.getClass())），按具体子类的 class 查 @TISExtension。

    public abstract static class BaseDescriptor extends Descriptor<ColumnWidth> {
        // 子类 Descriptor 可以在这里添加通用方法
    }
}