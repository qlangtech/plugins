package com.qlangtech.tis.plugin.ontology.impl.action;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.plugin.ds.JDBCTransactionScope;
import com.qlangtech.tis.plugin.ontology.impl.action.type.ActionType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.util.List;

/**
 * {@code DefaultOntologyAction.DefaultDesc#executeActionTypes} 的多步骤事务契约测试：
 * 不依赖 TIS 运行期 / 数据源，规则执行用 Mockito 桩。
 *
 * <p>钉住三条契约：
 * <ol>
 *   <li>全部规则在<b>同一个事务作用域</b>内按列表顺序执行，全部成功后统一 commit；</li>
 *   <li>任一条失败：异常透传、后面的规则不再执行 —— 与「按顺序编译成一次编辑」语义一致，
 *       失败时作用域 close 内部回滚（回滚行为由 tis-plugin 的
 *       {@code TestJDBCTransactionScope} 钉住，这里只验证调用序与作用域清理）；</li>
 *   <li>无论成败，线程本地作用域都不能残留 —— 否则当前线程后续所有
 *       {@code getConnection} 会永远走事务注册路径。</li>
 * </ol>
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/11/20
 */
public class TestMultiStepActionTransaction {

    private static final List<String> OBJECT_TYPE_REFS = List.of("Order");

    private final Describable<?> previewInstance = Mockito.mock(Describable.class);

    @After
    public void assertNoScopeLeak() {
        Assert.assertFalse("事务作用域不能残留在线程本地", JDBCTransactionScope.isActive());
    }

    @Test
    public void testAllRulesExecuteInOrderWithinSingleScope() {
        ActionType first = Mockito.mock(ActionType.class);
        ActionType second = Mockito.mock(ActionType.class);
        ActionType third = Mockito.mock(ActionType.class);

        DefaultOntologyAction.DefaultDesc.executeActionTypes(
                List.of(first, second, third), previewInstance, false, OBJECT_TYPE_REFS);

        // 按列表顺序逐条执行，且都携带同一个表单实例与命名空间上下文
        InOrder inOrder = Mockito.inOrder(first, second, third);
        inOrder.verify(first).executeActionRule(previewInstance, false, OBJECT_TYPE_REFS);
        inOrder.verify(second).executeActionRule(previewInstance, false, OBJECT_TYPE_REFS);
        inOrder.verify(third).executeActionRule(previewInstance, false, OBJECT_TYPE_REFS);
    }

    @Test
    public void testRuleFailureStopsSubsequentRulesAndPropagates() {
        ActionType first = Mockito.mock(ActionType.class);
        ActionType failing = Mockito.mock(ActionType.class);
        ActionType afterFailure = Mockito.mock(ActionType.class);
        Mockito.doThrow(new IllegalStateException("mock rule failure")).when(failing)
                .executeActionRule(previewInstance, false, OBJECT_TYPE_REFS);

        try {
            DefaultOntologyAction.DefaultDesc.executeActionTypes(
                    List.of(first, failing, afterFailure), previewInstance, false, OBJECT_TYPE_REFS);
            Assert.fail("rule failure must propagate");
        } catch (IllegalStateException expect) {
            // 异常透传给 httpProcess 的错误回显链路
        }

        Mockito.verify(first).executeActionRule(previewInstance, false, OBJECT_TYPE_REFS);
        Mockito.verify(failing).executeActionRule(previewInstance, false, OBJECT_TYPE_REFS);
        // 失败之后的规则不再执行
        Mockito.verify(afterFailure, Mockito.never())
                .executeActionRule(Mockito.any(), Mockito.anyBoolean(), Mockito.anyList());
    }

    @Test
    public void testEmptyRuleListStillOpensAndClosesScope() {
        // 空规则列表：开作用域、零次执行、commit 空作用域都是无操作，但不允许泄漏
        DefaultOntologyAction.DefaultDesc.executeActionTypes(
                List.of(), previewInstance, false, OBJECT_TYPE_REFS);
    }
}
