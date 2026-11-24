package com.qlangtech.tis.plugin.ontology.impl.action;

import com.qlangtech.tis.manage.common.Option;
import com.qlangtech.tis.plugin.ontology.OntologyObjectType;
import com.qlangtech.tis.plugin.ontology.OntologyProperty;
import com.qlangtech.tis.plugin.ontology.impl.action.type.ActionType;
import com.qlangtech.tis.plugin.ontology.impl.action.type.ObjectActionType;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

/**
 * 参数池候选跨规则联动（{@code ActionParameterConfig#collectCandidates}）的防退化钉子。
 *
 * <p>多对象类型 Action 下，主/副表共享属性会从两条规则各出一行候选——后贡献规则的那行
 * 必须带「已由 &lt;canonical 参数名&gt; 提供」标注，且勾选态只取自已持久化的参数池
 * （存量勾选不被强改、未持久化默认不勾）。
 */
public class TestActionParameterCandidates {

    /**
     * 主+副两规则共享 report_id/report_date：副表那两行候选带标注；主表与副表独有属性不带；
     * 主表已勾选的候选勾选态原样保留。
     */
    @Test
    public void sharedPropertyCandidatesAnnotatedWithCanonicalProvider() {
        ObjectActionType mainRule = spyRule("chartxy_daily_report",
                "report_id", "report_date", "department");
        ObjectActionType detailRule = spyRule("chartxy_daily_report_detail",
                "report_id", "report_date", "owner_email", "remark");
        Mockito.doReturn(candidates(mainRule, "chartxy_daily_report_",
                opt("report_id", true), opt("report_date", false), opt("department", true)))
                .when(mainRule).getEnumableCandidateSet(Mockito.any(), Mockito.anyList());
        Mockito.doReturn(candidates(detailRule, "chartxy_daily_report_detail_",
                opt("report_id", false), opt("report_date", false),
                opt("owner_email", true), opt("remark", false)))
                .when(detailRule).getEnumableCandidateSet(Mockito.any(), Mockito.anyList());

        List<Pair<Option, ActionParameterConfig>> collected = ActionParameterConfig.DftDescriptor.collectCandidates(
                List.of(mainRule, detailRule), null,
                List.of("chartxy_daily_report", "chartxy_daily_report_detail"));

        Assert.assertEquals("主表 3 + 副表 4，共享属性各出两行候选不去重", 7, collected.size());
        // 主表候选不带标注；已勾选的（report_id/department）勾选态原样保留
        assertLabel(collected.get(0), "report_id", true);
        assertLabel(collected.get(1), "report_date", false);
        assertLabel(collected.get(2), "department", true);
        // 副表共享属性带「已由 canonical 参数提供」标注
        assertLabel(collected.get(3), "report_id（已由 chartxy_daily_report_report_id 提供）", false);
        assertLabel(collected.get(4), "report_date（已由 chartxy_daily_report_report_date 提供）", false);
        // 副表独有属性不带标注
        assertLabel(collected.get(5), "owner_email", true);
        assertLabel(collected.get(6), "remark", false);
    }

    /**
     * 存量配置里已勾选副表共享属性时，标注不得强改勾选态（可覆盖的默认而非硬约束）。
     */
    @Test
    public void legacyCheckedDuplicateStaysChecked() {
        ObjectActionType mainRule = spyRule("chartxy_daily_report", "report_id");
        ObjectActionType detailRule = spyRule("chartxy_daily_report_detail", "report_id");
        Mockito.doReturn(candidates(mainRule, "chartxy_daily_report_", opt("report_id", true)))
                .when(mainRule).getEnumableCandidateSet(Mockito.any(), Mockito.anyList());
        Mockito.doReturn(candidates(detailRule, "chartxy_daily_report_detail_", opt("report_id", true)))
                .when(detailRule).getEnumableCandidateSet(Mockito.any(), Mockito.anyList());

        List<Pair<Option, ActionParameterConfig>> collected = ActionParameterConfig.DftDescriptor.collectCandidates(
                List.of(mainRule, detailRule), null,
                List.of("chartxy_daily_report", "chartxy_daily_report_detail"));

        Assert.assertTrue("已持久化的勾选不被强改",
                collected.get(1).getLeft().isChecked());
        assertLabel(collected.get(1), "report_id（已由 chartxy_daily_report_report_id 提供）", true);
    }

    /**
     * 单对象类型（前缀为空）退化为裸参数名、canonical 参数名与参数名逐字相同：不产生任何标注，
     * 存量单对象 Action 的候选列表行为逐字不变。
     */
    @Test
    public void singleNamespaceCandidatesStayUnannotated() {
        ObjectActionType userRule = spyRule("user", "id", "name");
        Mockito.doReturn(candidates(userRule, "", opt("id", true), opt("name", false)))
                .when(userRule).getEnumableCandidateSet(Mockito.any(), Mockito.anyList());

        List<Pair<Option, ActionParameterConfig>> collected = ActionParameterConfig.DftDescriptor.collectCandidates(
                List.of(userRule), null, List.of("user"));

        assertLabel(collected.get(0), "id", true);
        assertLabel(collected.get(1), "name", false);
        // 合并后重新按出现顺序编号
        Assert.assertEquals(Integer.valueOf(0), collected.get(0).getRight().order);
        Assert.assertEquals(Integer.valueOf(1), collected.get(1).getRight().order);
    }

    private static ObjectActionType spyRule(String objectTypeRef, String... props) {
        ObjectActionType rule = Mockito.spy(new ObjectActionType());
        rule.objectTypeReference = objectTypeRef;
        Mockito.doReturn(objectTypeWith(props)).when(rule).loadObjectType();
        return rule;
    }

    private static OntologyObjectType objectTypeWith(String... props) {
        OntologyObjectType objectType = Mockito.mock(OntologyObjectType.class);
        List<OntologyProperty> cols = new ArrayList<>();
        for (String prop : props) {
            OntologyProperty property = Mockito.mock(OntologyProperty.class);
            Mockito.when(property.getName()).thenReturn(prop);
            cols.add(property);
        }
        Mockito.when(objectType.getCols()).thenReturn(cols);
        return objectType;
    }

    /**
     * 组造一条规则的候选列表：parameterId = 前缀 + 属性名（与
     * {@code ObjectActionType#getEnumableCandidateSet} 的产出口径一致）。
     */
    private static List<Pair<Option, ActionParameterConfig>> candidates(
            ObjectActionType rule, String prefix, Option... opts) {
        List<Pair<Option, ActionParameterConfig>> candidates = new ArrayList<>();
        int order = 0;
        for (Option opt : opts) {
            ActionParameterConfig config = new ActionParameterConfig();
            config.parameterId = prefix + opt.getName();
            config.order = order++;
            candidates.add(Pair.of(opt, config));
        }
        return candidates;
    }

    private static Option opt(String name, boolean checked) {
        return new Option(name, name).setChecked(checked);
    }

    private static void assertLabel(Pair<Option, ActionParameterConfig> candidate,
                                    String expectedLabel, boolean expectedChecked) {
        Assert.assertEquals("候选展示名：" + candidate.getLeft().getName(),
                expectedLabel, candidate.getLeft().getName());
        Assert.assertEquals(expectedLabel, expectedChecked, candidate.getLeft().isChecked());
    }
}
