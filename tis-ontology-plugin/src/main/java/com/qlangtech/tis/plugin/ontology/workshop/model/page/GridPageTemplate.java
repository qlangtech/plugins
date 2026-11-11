package com.qlangtech.tis.plugin.ontology.workshop.model.page;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;

/**
 * 网格布局模板：自适应多列网格，列数由容器宽度决定。
 *
 * <p>对应原枚举的 GRID，适合只读监控看板 —— 指标卡 + 图表/表格铺成网格，基本无写入交互。
 * 对应 Foundry 的 Grid 与 Design Hub 的 Metrics Dashboard。
 *
 * <h3>使用场景</h3>
 * <ul>
 *   <li><b>监控大盘</b>：多个指标卡自动排列，屏幕越宽显示越多列
 *   <li><b>图表集合</b>：多个图表/表格铺成响应式网格
 *   <li><b>卡片墙</b>：多个卡片组件（对象卡片、统计卡片等）自动排列
 * </ul>
 *
 * <h3>配置项</h3>
 * <ul>
 *   <li><b>最小列宽</b>（{@link #minColumnWidth}）：单个网格项的最小宽度，默认 280px。
 *       浏览器会根据容器宽度自动计算能放下几列：
 *       <ul>
 *         <li>容器 1200px、最小列宽 280px → 4 列（每列 300px）
 *         <li>容器 800px、最小列宽 280px → 2 列（每列 400px）
 *         <li>容器 500px、最小列宽 280px → 1 列（500px）
 *       </ul>
 *   <li><b>首项通栏</b>（{@link #firstChildSpanAll}）：是否让第一个 Section 占满所有列，
 *       默认 {@code true}。通常用于放置全局筛选器，下方才是网格区块。
 * </ul>
 *
 * <h3>CSS 输出</h3>
 * <p>使用 CSS Grid 的 {@code auto-fit} + {@code minmax}：
 * <pre>{@code
 * grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
 * }</pre>
 * 这是响应式网格的标准写法：
 * <ul>
 *   <li>{@code auto-fit}：自动计算列数，容不下时自动换行
 *   <li>{@code minmax(280px, 1fr)}：每列最小 280px、最大平分剩余空间
 * </ul>
 *
 * <h3>注意事项</h3>
 * <ul>
 *   <li><b>不回落单栏</b>：网格布局本身就是响应式的，窄屏会自动变成 1-2 列，
 *       不需要强制单栏（窄屏回落由前端 {@code workshop-page.component.less} 统一处理）
 *   <li><b>建页默认铺 5 个 Section</b>：首项通栏（筛选器）+ 4 个网格块，
 *       用户可以随时增删
 * </ul>
 *
 * @see PageTemplate
 */
public class GridPageTemplate extends PageTemplate {

    private static final long serialVersionUID = 1L;

    /**
     * 单个网格项的最小宽度（单位：px）。
     *
     * <p>取值范围：200–500（过窄卡片挤不下内容，过宽会让大屏只显示 1-2 列）。
     * 缺省值 280（原 GRID 的默认值）。
     */
    @FormField(ordinal = 0, type = FormFieldType.INT_NUMBER, validate = {Validator.require})
    public Integer minColumnWidth = 280;

    /**
     * 是否让首个 Section 通栏（占满所有列）。
     *
     * <p>缺省 {@code true}，对应原 GRID 的默认行为：首项放筛选器，下方是网格区块。
     */
    @FormField(ordinal = 1, type = FormFieldType.ENUM, validate = {Validator.require})
    public Boolean firstChildSpanAll = true;

    @TISExtension
    public static class DefaultDescriptor extends PageTemplate.BaseDescriptor  {
        public String getDisplayName() {
            return "Grid_Page_Template";
        }

        @Override
        public String shortComment() {
            return "网格布局";
        }

        @Override
        public EndType getEndType() {
            return EndType.LayoutGrid;
        }

        /**
         * 校验网格项最小列宽取值范围（200–500）。
         *
         * <p>范围来自 {@link #minColumnWidth} 的 javadoc：过窄卡片挤不下内容，
         * 过宽会让大屏只显示 1-2 列，失去网格的意义。
         */
        public boolean validateMinColumnWidth(IFieldErrorHandler msgHandler, Context context,
                                              String fieldName, String value) {
            int minColumnWidth = Integer.parseInt(value);
            if (minColumnWidth < 200 || minColumnWidth > 500) {
                msgHandler.addFieldError(context, fieldName,
                        "最小列宽必须在 200 至 500 像素之间（过窄卡片挤不下内容，过宽会让大屏只显示 1-2 列）");
                return false;
            }
            return true;
        }
    }
}
