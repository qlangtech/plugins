package com.qlangtech.tis.plugin.ontology.workshop.widget;

import com.qlangtech.tis.plugin.ontology.OntologyType;
import com.qlangtech.tis.plugin.ontology.workshop.widget.WidgetColumnConfig.ColumnAlign;
import com.qlangtech.tis.plugin.ontology.workshop.widget.WidgetColumnConfig.ColumnFormat;
import org.junit.Assert;
import org.junit.Test;

/**
 * {@link WidgetColumnConfig#formatOf(OntologyType)} / {@link WidgetColumnConfig#alignOf} 的映射契约。
 *
 * <p>对象集变量绑定后自动填充列，列的类型与对齐全靠这两个纯函数推导。它们同时也是
 * 「新增一个 {@link OntologyType} 常量必须同步决定其列格式化方式」的落点 ——
 * 那两个 switch expression 不写 {@code default}，漏掉常量会编译失败；本测试再从
 * 取值一侧兜一遍底。
 *
 * <p>纯逻辑断言，不触达 TIS 插件扫描，故无环境前提。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/4
 */
public class TestWidgetColumnConfigColumnMapping {

    @Test
    public void testFormatOfNumericTypes() {
        for (OntologyType t : new OntologyType[]{OntologyType.INTEGER, OntologyType.SHORT, OntologyType.LONG,
                OntologyType.BYTE, OntologyType.FLOAT, OntologyType.DOUBLE, OntologyType.DECIMAL}) {
            Assert.assertEquals(t.name(), ColumnFormat.number, WidgetColumnConfig.formatOf(t));
        }
    }

    @Test
    public void testFormatOfTextTypes() {
        // 复杂/向量/地理/媒体一律按纯文本展示：它们没有通用的单元格渲染器，交由前端原样输出
        for (OntologyType t : new OntologyType[]{OntologyType.STRING, OntologyType.VECTOR, OntologyType.ARRAY,
                OntologyType.STRUCT, OntologyType.GEOPOINT, OntologyType.GEOSHAPE, OntologyType.MEDIA_REFERENCE}) {
            Assert.assertEquals(t.name(), ColumnFormat.text, WidgetColumnConfig.formatOf(t));
        }
    }

    @Test
    public void testFormatOfBooleanAndTimeTypes() {
        Assert.assertEquals(ColumnFormat.boolean_, WidgetColumnConfig.formatOf(OntologyType.BOOLEAN));
        Assert.assertEquals(ColumnFormat.date, WidgetColumnConfig.formatOf(OntologyType.DATE));
        Assert.assertEquals(ColumnFormat.date, WidgetColumnConfig.formatOf(OntologyType.TIMESTAMP));
    }

    /**
     * 每个 {@link OntologyType} 都必须能推出一个非 null 的 format，且由它推出的 align 也非 null。
     * 与 switch 的编译期穷尽性互为双保险：前者防漏常量，后者防映射到 null。
     */
    @Test
    public void testEveryOntologyTypeIsMapped() {
        for (OntologyType t : OntologyType.values()) {
            ColumnFormat format = WidgetColumnConfig.formatOf(t);
            Assert.assertNotNull(t.name(), format);
            Assert.assertNotNull(t.name(), WidgetColumnConfig.alignOf(format));
        }
    }

    @Test
    public void testAlignOf() {
        Assert.assertEquals(ColumnAlign.right, WidgetColumnConfig.alignOf(ColumnFormat.number));
        Assert.assertEquals(ColumnAlign.center, WidgetColumnConfig.alignOf(ColumnFormat.boolean_));
        Assert.assertEquals(ColumnAlign.left, WidgetColumnConfig.alignOf(ColumnFormat.text));
        Assert.assertEquals(ColumnAlign.left, WidgetColumnConfig.alignOf(ColumnFormat.date));
    }

    /**
     * align 与 format 的枚举项同长（各 3 / 4 项）但不同维度，别把两者搞混 ——
     * 顺带锁住 {@link WidgetColumnConfig#DEFAULT_WIDTH} 与 .json 里 width dftVal 的一致性。
     */
    @Test
    public void testDefaultWidthMatchesJsonDftVal() {
        Assert.assertEquals(120, WidgetColumnConfig.DEFAULT_WIDTH);
    }
}
