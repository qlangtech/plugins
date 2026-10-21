package com.qlangtech.tis.plugin.ontology.impl.action.type;

import com.qlangtech.tis.plugin.ontology.impl.action.rule.CreateLinkRule;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.DeleteLinkRule;
import org.junit.Assert;
import org.junit.Test;

/**
 * 链接规则的纯逻辑单测：不依赖 TIS 运行期 / 数据源。
 *
 * <p>链路：{@code LinkActionType#executeActionRule} 先解析链接类型与两个 RID 参数，
 * 再由 {@link LinkActionExecutor} 按 {@code RelationshipType} 落表。这里覆盖解析与拒绝分支，
 * 真正的 DML 由端到端用例覆盖。
 */
public class TestLinkRuleSupport {

    @Test
    public void testActionLevelLinkTypeWins() {
        CreateLinkRule rule = new CreateLinkRule();
        // 旧配置的 linkTypeName 为空 / 与 Action 级一致，都按 Action 级执行
        Assert.assertEquals("Order_Customer",
                LinkActionType.resolveLinkTypeId("Order_Customer", rule));
        rule.linkTypeName = "Order_Customer";
        Assert.assertEquals("Order_Customer",
                LinkActionType.resolveLinkTypeId("Order_Customer", rule));
    }

    @Test
    public void testLegacyLinkTypeNameFallback() {
        DeleteLinkRule rule = new DeleteLinkRule();
        rule.linkTypeName = "Order_Customer";
        // Action 级为空时回退旧值，兼容升级前保存的 Action
        Assert.assertEquals("Order_Customer",
                LinkActionType.resolveLinkTypeId(null, rule));
        Assert.assertEquals("Order_Customer",
                LinkActionType.resolveLinkTypeId("  ", rule));
    }

    @Test
    public void testMismatchLinkTypeThrows() {
        CreateLinkRule rule = new CreateLinkRule();
        rule.linkTypeName = "Legacy_Link";
        IllegalStateException e = Assert.assertThrows(IllegalStateException.class,
                () -> LinkActionType.resolveLinkTypeId("New_Link", rule));
        Assert.assertTrue(e.getMessage().contains("Legacy_Link"));
        Assert.assertTrue(e.getMessage().contains("New_Link"));
    }

    @Test
    public void testMissingLinkTypeThrows() {
        Assert.assertThrows(IllegalStateException.class,
                () -> LinkActionType.resolveLinkTypeId(null, new CreateLinkRule()));
        Assert.assertThrows(IllegalStateException.class,
                () -> LinkActionType.resolveLinkTypeId(null, new DeleteLinkRule()));
    }

    @Test
    public void testRidParamNames() {
        CreateLinkRule create = new CreateLinkRule();
        Assert.assertEquals("sourceRid", LinkActionType.sourceRidParamName(create));
        Assert.assertEquals("targetRid", LinkActionType.targetRidParamName(create));

        DeleteLinkRule delete = new DeleteLinkRule();
        delete.sourceObjectRidParam = "fromRid";
        delete.targetObjectRidParam = "toRid";
        Assert.assertEquals("fromRid", LinkActionType.sourceRidParamName(delete));
        Assert.assertEquals("toRid", LinkActionType.targetRidParamName(delete));
    }

    @Test
    public void testEmptyRidParamNameThrows() {
        CreateLinkRule rule = new CreateLinkRule();
        rule.sourceObjectRidParam = " ";
        Assert.assertThrows(IllegalStateException.class,
                () -> LinkActionType.sourceRidParamName(rule));
    }

    @Test
    public void testBidirectionalRejected() {
        Assert.assertThrows(UnsupportedOperationException.class,
                () -> LinkActionExecutor.rejectBidirectional(Boolean.TRUE));
        // false / null 不拦截
        LinkActionExecutor.rejectBidirectional(Boolean.FALSE);
        LinkActionExecutor.rejectBidirectional(null);
    }

    @Test
    public void testValueComparison() {
        Assert.assertTrue(LinkActionExecutor.sameValue(1, 1L));
        Assert.assertTrue(LinkActionExecutor.sameValue("12", 12));
        Assert.assertTrue(LinkActionExecutor.sameValue("12.0", "12"));
        Assert.assertFalse(LinkActionExecutor.sameValue("A", "a"));
        Assert.assertFalse(LinkActionExecutor.sameValue(null, "x"));
        Assert.assertTrue(LinkActionExecutor.isBlankValue("  "));
        Assert.assertFalse(LinkActionExecutor.isBlankValue(0));
    }
}
