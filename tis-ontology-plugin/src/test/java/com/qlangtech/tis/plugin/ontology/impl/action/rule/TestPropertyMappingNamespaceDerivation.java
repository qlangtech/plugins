package com.qlangtech.tis.plugin.ontology.impl.action.rule;

import com.qlangtech.tis.manage.common.Option;
import com.qlangtech.tis.plugin.IPluginStore;
import com.qlangtech.tis.plugin.ontology.OntologyProperty;
import com.qlangtech.tis.plugin.ontology.OntologyType;
import com.qlangtech.tis.plugin.ontology.impl.action.type.ActionType;
import com.qlangtech.tis.plugin.ontology.impl.action.type.ObjectActionType;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 * 「表单态 vs 磁盘态」命名空间推导（{@code PropertyMappingConfig.DftDescriptor#deriveNamespaceCtx}）
 * 防退化钉子。
 *
 * <p>映射表候选生成（GetDesc）发生在规则<b>保存前</b>，磁盘上的 Action 规则列表天然落后于编辑中
 * 的表单。2026/11/24 线上实证：给单对象规则 Action 追加第二条对象规则时，新规则的 ref 只存在于
 * 解析上下文（posted host item），磁盘 refs 塌缩成 1 → ownPrefix 恒空串、canonical 全裸名 →
 * 默认行保存后全是死行。修复是「磁盘 refs ∪ 编辑中 ownRef」，本类钉住该并集在四种场景下的语义。
 */
public class TestPropertyMappingNamespaceDerivation {

    /** 主表规则 spy：objectTypeReference 字段可用真实例赋值，loadObjectType 摆脱线程上下文 stub 掉 */
    private static ObjectActionType spyRule(String ruleName, String objectType, String... props) {
        ObjectActionType rule = Mockito.spy(ruleOf(ruleName, objectType));
        Mockito.doReturn(objectTypeWith(props)).when(rule).loadObjectType();
        return rule;
    }

    private static ObjectActionType ruleOf(String ruleName, String objectType) {
        ObjectActionType actionType = new ObjectActionType();
        actionType.ruleName = ruleName;
        actionType.objectTypeReference = objectType;
        actionType.objectRule = new CreateObjectRule();
        return actionType;
    }

    private static com.qlangtech.tis.plugin.ontology.OntologyObjectType objectTypeWith(String... props) {
        com.qlangtech.tis.plugin.ontology.OntologyObjectType objectType =
                Mockito.mock(com.qlangtech.tis.plugin.ontology.OntologyObjectType.class);
        List<OntologyProperty> cols = new java.util.ArrayList<>();
        for (String prop : props) {
            OntologyProperty property = Mockito.mock(OntologyProperty.class);
            Mockito.when(property.getName()).thenReturn(prop);
            cols.add(property);
        }
        Mockito.when(objectType.getCols()).thenReturn(cols);
        return objectType;
    }

    private static OntologyProperty prop(String name, OntologyType type) {
        OntologyProperty col = Mockito.mock(OntologyProperty.class);
        Mockito.when(col.getName()).thenReturn(name);
        Mockito.when(col.parseOntologyType()).thenReturn(type);
        return col;
    }

    private static String refOf(Pair<Option, IPluginStore.MultiDescribleElement> candidate) {
        ValueSourceConfig source = ((PropertyMappingConfig) candidate.getRight()).source;
        Assert.assertSame("默认来源应为参数引用来源：" + source.getClass(),
                FromParameterValueSource.class, source.getClass());
        return ((FromParameterValueSource) source).parameterRef;
    }

    /**
     * 核心钉子（本次线上实证的 bug 本体）：磁盘只有主表规则，编辑中的副表规则尚未落盘。
     * 并集修复前 refs=[chartxy_daily_report] 塌缩成 1 → ownPrefix=""、canonical 裸名，
     * 副表默认行保存后全部成为死行。
     */
    @Test
    public void appendingSecondObjectRuleDerivesNamespaceFromFormStateNotDiskState() {
        ObjectActionType mainRule = spyRule("chartxy_daily_report_create"
                , "chartxy_daily_report", "report_id", "report_date", "department");

        // 磁盘：只有主表规则；表单：正在配置副表规则（ref 只在解析上下文里，尚未保存）
        PropertyMappingConfig.DftDescriptor.NamespaceCtx ctx
                = PropertyMappingConfig.DftDescriptor.deriveNamespaceCtx(
                List.of(mainRule), "chartxy_daily_report_detail");

        Assert.assertEquals("编辑中规则的前缀按「保存后形态」推导，不得塌缩成空串",
                "chartxy_daily_report_detail_", ctx.ownPrefix());
        Assert.assertEquals("主表规则前缀按并集 refs（size=2）计算，canonical 带主表命名空间",
                "chartxy_daily_report_report_id", ctx.canonicalParams().get("report_id"));
        Assert.assertEquals("chartxy_daily_report_report_date", ctx.canonicalParams().get("report_date"));
        Assert.assertEquals("chartxy_daily_report_department", ctx.canonicalParams().get("department"));

        // 端到端：副表默认行——共享属性引主表 canonical 参数，非共享属性引本规则命名空间参数
        List<Pair<Option, IPluginStore.MultiDescribleElement>> candidates
                = PropertyMappingConfig.DftDescriptor.buildCandidates(
                List.of(prop("report_id", OntologyType.STRING),
                        prop("owner_email", OntologyType.STRING)),
                Set.of(), ctx.canonicalParams(), ctx.ownPrefix());
        Assert.assertEquals("共享属性默认行引主表 canonical 参数",
                "chartxy_daily_report_report_id", refOf(candidates.get(0)));
        Assert.assertEquals("非共享属性默认行引本规则命名空间参数",
                "chartxy_daily_report_detail_owner_email", refOf(candidates.get(1)));
    }

    /**
     * 已落盘双规则编辑首条规则：ownRef 本就在 refs 里，并集无变化；
     * 共享属性 canonical == 本规则自己的命名空间参数名（首个贡献者）。
     */
    @Test
    public void editingSavedFirstRuleRefersOwnNamespacedParam() {
        ObjectActionType mainRule = spyRule("createMain", "chartxy_daily_report", "report_id");
        ObjectActionType detailRule = spyRule("createDetail"
                , "chartxy_daily_report_detail", "report_id", "owner_email");

        PropertyMappingConfig.DftDescriptor.NamespaceCtx ctx
                = PropertyMappingConfig.DftDescriptor.deriveNamespaceCtx(
                List.of(mainRule, detailRule), "chartxy_daily_report");

        Assert.assertEquals("chartxy_daily_report_", ctx.ownPrefix());
        Assert.assertEquals("共享属性归首个贡献规则",
                "chartxy_daily_report_report_id", ctx.canonicalParams().get("report_id"));
        Assert.assertEquals("canonical 即本规则自己的参数（== ownPrefix + 属性名）→ 引用自己的",
                ctx.ownPrefix() + "report_id", ctx.canonicalParams().get("report_id"));
    }

    /**
     * 创建向导配置首条规则（Action 尚未落盘）：并集后 refs=[ownRef] 仍为单对象类型，
     * 退化裸名旧行为——调用侧 action==null 映射成空列表，null 同样按空列表处理。
     */
    @Test
    public void wizardFirstRuleFallsBackToLegacyBareNames() {
        PropertyMappingConfig.DftDescriptor.NamespaceCtx ctx
                = PropertyMappingConfig.DftDescriptor.deriveNamespaceCtx(
                List.of(), "chartxy_daily_report");
        Assert.assertEquals("单对象类型前缀为空串（设计行为）", "", ctx.ownPrefix());
        Assert.assertTrue("无落盘规则即无 canonical 贡献", ctx.canonicalParams().isEmpty());

        PropertyMappingConfig.DftDescriptor.NamespaceCtx fromNull
                = PropertyMappingConfig.DftDescriptor.deriveNamespaceCtx(null, "chartxy_daily_report");
        Assert.assertEquals("null 规则列表等价空列表", "", fromNull.ownPrefix());
    }

    /**
     * ownRef 为空不得并进 refs：否则 refs 假性变 2，落盘规则的前缀会被错误打开。
     * （现网该入参必非空——候选生成上游已据此加载过 cols——此处钉的是纯函数的防御语义。）
     */
    @Test
    public void emptyOwnRefNotMergedSoPersistedRuleStaysUnnamespaced() {
        ObjectActionType mainRule = spyRule("only", "chartxy_daily_report", "report_id");

        PropertyMappingConfig.DftDescriptor.NamespaceCtx ctx
                = PropertyMappingConfig.DftDescriptor.deriveNamespaceCtx(List.of(mainRule), null);

        Assert.assertEquals("", ctx.ownPrefix());
        Assert.assertEquals("refs 保持单对象类型：主表属性保持裸名",
                "report_id", ctx.canonicalParams().get("report_id"));
    }

    /**
     * 磁盘规则里的非对象规则（链接/函数等）不参与 refs；剔除后塌缩的 refs 由 ownRef 并集兜住。
     */
    @Test
    public void nonObjectRulesFilteredBeforeUnion() {
        ObjectActionType mainRule = spyRule("createMain", "chartxy_daily_report", "report_id");

        PropertyMappingConfig.DftDescriptor.NamespaceCtx ctx
                = PropertyMappingConfig.DftDescriptor.deriveNamespaceCtx(
                Arrays.asList(Mockito.mock(ActionType.class), mainRule), "chartxy_daily_report_detail");

        Assert.assertEquals("非对象规则被过滤后，磁盘 refs=[chartxy_daily_report] 塌缩成 1，"
                + "并集 ownRef 后命名空间判定不受影响", "chartxy_daily_report_detail_", ctx.ownPrefix());
        Assert.assertEquals("chartxy_daily_report_report_id", ctx.canonicalParams().get("report_id"));
    }
}
