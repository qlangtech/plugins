package com.qlangtech.tis.plugin.ontology.impl.action.rule;

import com.qlangtech.tis.manage.common.Option;
import com.qlangtech.tis.plugin.IPluginStore;
import com.qlangtech.tis.plugin.ontology.OntologyProperty;
import com.qlangtech.tis.plugin.ontology.OntologyType;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 映射默认行的命名空间感知（{@code PropertyMappingConfig.DftDescriptor} 的
 * {@code buildCandidates}/{@code defaultSource}）防退化钉子。
 *
 * <p>多对象类型 Action 下共享属性（主/副表都有 report_id）的默认来源必须引用先贡献规则
 * 提供的 canonical 参数——这是「参数池共享字段 + 映射行补值」人工方案的自动形态；
 * canonical 不可用（半配置回退）时旧行为逐字不变。
 */
public class TestPropertyMappingDefaultSource {

    /**
     * 共享属性默认行引用 canonical 参数：report_id（STRING）与 report_date（DATE）都指向
     * 主表命名空间参数——尤其 DATE 型共享属性不得默认成 CurrentTimeValueSource，
     * 否则副表时间戳就不与主表同值了。
     */
    @Test
    public void sharedPropertyDefaultsRefersCanonicalParam() {
        Map<String, String> canonical = Map.of(
                "report_id", "chartxy_daily_report_report_id",
                "report_date", "chartxy_daily_report_report_date");

        List<Pair<Option, IPluginStore.MultiDescribleElement>> candidates
                = PropertyMappingConfig.DftDescriptor.buildCandidates(
                List.of(prop("report_id", OntologyType.STRING),
                        prop("report_date", OntologyType.DATE),
                        prop("owner_email", OntologyType.STRING)),
                Set.of(), canonical, "chartxy_daily_report_detail_");

        Assert.assertEquals(3, candidates.size());
        Assert.assertEquals("共享 STRING 属性 → canonical 参数",
                "chartxy_daily_report_report_id", refOf(candidates.get(0)));
        Assert.assertEquals("共享 DATE 属性同样引用 canonical 参数，不得默认成当前时间",
                "chartxy_daily_report_report_date", refOf(candidates.get(1)));
        Assert.assertEquals("非共享属性引用本规则命名空间参数",
                "chartxy_daily_report_detail_owner_email", refOf(candidates.get(2)));
    }

    /**
     * 本规则自己就是 canonical 贡献者（canonical 参数名 == 本规则前缀参数名）：
     * 不算共享，引用自己的命名空间参数（单对象类型时前缀为空串，退化为裸名——旧行为）。
     */
    @Test
    public void ownCanonicalPropertyRefersOwnNamespacedParam() {
        Map<String, String> canonical = Map.of("amount", "chartxy_daily_report_amount");

        ValueSourceConfig source = PropertyMappingConfig.DftDescriptor.defaultSource(
                "amount", OntologyType.STRING, canonical, "chartxy_daily_report_");

        Assert.assertEquals("canonical 参数即本规则自己的参数，引用之",
                "chartxy_daily_report_amount", ((FromParameterValueSource) source).parameterRef);
    }

    /**
     * 非共享的日期属性保持「当前时间」默认（现行为）。
     */
    @Test
    public void unsharedDatePropertyKeepsCurrentTimeDefault() {
        ValueSourceConfig source = PropertyMappingConfig.DftDescriptor.defaultSource(
                "created_at", OntologyType.TIMESTAMP,
                Map.of("created_at", "chartxy_daily_report_created_at"), "chartxy_daily_report_");

        Assert.assertSame("非共享日期属性默认当前时间", CurrentTimeValueSource.class, source.getClass());
    }

    /**
     * canonical 不可用（半配置回退：空 map）时旧行为逐字不变——裸名参数 + 日期当前时间。
     */
    @Test
    public void fallbackToLegacyBareNamesWhenCanonicalUnavailable() {
        List<Pair<Option, IPluginStore.MultiDescribleElement>> candidates
                = PropertyMappingConfig.DftDescriptor.buildCandidates(
                List.of(prop("report_id", OntologyType.STRING),
                        prop("report_date", OntologyType.DATE)),
                Set.of(), Map.of(), "");

        Assert.assertEquals("回退旧行为：STRING → 裸名参数",
                "report_id", refOf(candidates.get(0)));
        Assert.assertSame("回退旧行为：DATE → 当前时间",
                CurrentTimeValueSource.class,
                ((PropertyMappingConfig) candidates.get(1).getRight()).source.getClass());
    }

    /**
     * 勾选态只取已持久化的映射行：已配置的 target 勾选、其余不勾（不被自动预置勾上）。
     */
    @Test
    public void persistedTargetsPreserveCheckedState() {
        List<Pair<Option, IPluginStore.MultiDescribleElement>> candidates
                = PropertyMappingConfig.DftDescriptor.buildCandidates(
                List.of(prop("report_id", OntologyType.STRING),
                        prop("owner_email", OntologyType.STRING)),
                Set.of("report_id"), Map.of("report_id", "chartxy_daily_report_report_id"),
                "chartxy_daily_report_detail_");

        Assert.assertTrue("已持久化的映射行勾选保留", candidates.get(0).getLeft().isChecked());
        Assert.assertFalse("未配置行默认不勾，自动预置只生成默认来源", candidates.get(1).getLeft().isChecked());
    }

    private static OntologyProperty prop(String name, OntologyType type) {
        OntologyProperty col = Mockito.mock(OntologyProperty.class);
        Mockito.when(col.getName()).thenReturn(name);
        Mockito.when(col.parseOntologyType()).thenReturn(type);
        return col;
    }

    private static String refOf(Pair<Option, IPluginStore.MultiDescribleElement> candidate) {
        ValueSourceConfig source = ((PropertyMappingConfig) candidate.getRight()).source;
        Assert.assertSame("共享/非共享属性的默认来源应为参数引用来源：" + source.getClass(),
                FromParameterValueSource.class, source.getClass());
        return ((FromParameterValueSource) source).parameterRef;
    }
}
