package com.qlangtech.tis.plugin.ontology.impl.action.rule;

import com.qlangtech.tis.manage.common.Option;
import com.qlangtech.tis.plugin.datax.transformer.UDFDesc;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

/**
 * {@code OntologyActionRule} 子类 literia 输出契约测试。
 *
 * <p>前端 tuples 视图（tis-console 的 {@code udf-desc-literia} 组件）按
 * 「{@code key: value}，value 为数组时递归」的线格式渲染 {@code List<UDFDesc>}，
 * 本轮给 8 条规则补齐 {@code getLiteria()}，嵌套配置走委托模式。改动面上几乎
 * 每一处都没有编译期信号：
 *
 * <ol>
 *   <li>「首对必是 ruleType」是前端可读性契约，写漏一条该规则的描述就缺类型标签；</li>
 *   <li>半配置状态（映射行未配 / 渠道未选 / 策略未选）literia 必须空安全，
 *       {@code ObjectActionType#getLiteria} 在取数链路上直接遍历它；</li>
 *   <li>委托深度（映射行 → 取值来源、渠道、重试策略）断言错层级会在前端渲染成
 *       「少摊一层恒空」。</li>
 * </ol>
 *
 * <p>本类不依赖 TIS 运行期：纯 {@code new} 规则实例 + 字段赋值，不触
 * {@code getDescriptor()}，无需 {@code --add-opens}。
 */
public class TestOntologyActionRuleLiteria {

    // ===================================================================
    // 1. ruleType 首对契约
    // ===================================================================

    /**
     * 8 条规则的首对都必须是 {@code ruleType=<枚举名>}，且值与 {@code getRuleType()} 同源——
     * 两个字段来源不同（一个硬编码、一个枚举方法）就会各自漂移。
     */
    @Test
    public void ruleTypeIsFirstPairForAllRules() {
        List<OntologyActionRule> rules = List.of(//
                new CreateObjectRule() //
                , new ModifyObjectRule() //
                , new DeleteObjectRule() //
                , new CreateLinkRule() //
                , new DeleteLinkRule() //
                , new FunctionBackedRule() //
                , new NotificationRule() //
                , new WebhookRule());
        for (OntologyActionRule rule : rules) {
            List<UDFDesc> literia = rule.getLiteria();
            Assert.assertFalse(rule.getClass().getSimpleName() + " 的 literia 不能为空", literia.isEmpty());
            Option first = literia.get(0).getPairs().get(0);
            Assert.assertEquals(rule.getClass().getSimpleName() + " 首对键必须是规则类型标签",
                    OntologyActionRule.KEY_RULE_TYPE, first.getName());
            Assert.assertEquals(rule.getClass().getSimpleName() + " 首对值必须与 getRuleType() 同源",
                    rule.getRuleType().name(), first.getValue());
        }
    }

    // ===================================================================
    // 2. 半配置空安全
    // ===================================================================

    /**
     * 全未配置的规则 literia 不能抛：{@code ObjectActionType#getLiteria} 在取数链路上
     * 直接 {@code addAll(objectRule.getLiteria())}，半配置抛了整张表单打不开。
     */
    @Test
    public void unconfiguredRulesAreNullSafe() {
        Assert.assertNotNull(new CreateObjectRule().getLiteria());
        Assert.assertNotNull(new FunctionBackedRule().getLiteria());

        // mappings（CreateObjectRule 的 literia 键）未配置 → 空列表，而非 null
        CreateObjectRule createRule = new CreateObjectRule();
        Option mappings = pair(createRule.getLiteria(), PropertyMappingConfig.KEY_MAPPINGS);
        Assert.assertNotNull(mappings);
        Assert.assertTrue("未配置的映射行应描述成空列表",
                ((List<?>) mappings.getValue()).isEmpty());

        // 渠道未选 → 不下发 notifyChannel 该对，而非 null 值对
        NotificationRule notification = new NotificationRule();
        Assert.assertNull("渠道未选时不应有 notifyChannel 对",
                pair(notification.getLiteria(), "notifyChannel"));
    }

    // ===================================================================
    // 3. 委托深度
    // ===================================================================

    /**
     * 映射行委托两层：规则 → {@code PropertyMappingConfig}（target 作键）→
     * {@code ValueSourceConfig} 子类各自的描述。
     */
    @Test
    public void mappingRowDelegatesToValueSource() {
        CreateObjectRule rule = new CreateObjectRule();
        PropertyMappingConfig mapping = new PropertyMappingConfig();
        mapping.target = "report_id";
        mapping.source = new FromParameterValueSource("p1");
        rule.propertyMappings = Collections.singletonList(mapping);

        List<UDFDesc> rowLiteria = nested(pair(rule.getLiteria(), PropertyMappingConfig.KEY_MAPPINGS));
        Assert.assertEquals("一行映射产出一条描述", 1, rowLiteria.size());
        Option targetPair = rowLiteria.get(0).getPairs().get(0);
        Assert.assertEquals("target 作行键", "report_id", targetPair.getName());

        List<UDFDesc> sourceLiteria = nested(targetPair);
        Option paramPair = pair(sourceLiteria, "parameterRef");
        Assert.assertNotNull("参数引用来源应下发 parameterRef 对", paramPair);
        Assert.assertEquals("参数引用包一层 ${} 与字面量区分开", "${p1}", paramPair.getValue());
    }

    /**
     * 三条持有映射行的规则在 literia 里用同一个键 {@link PropertyMappingConfig#KEY_MAPPINGS}。
     * 曾经三处各自为政（mappings / propertyMappings / parameterMappings），前端 tuples 视图
     * 按列名对齐三处映射配置时对不上；统一后由本用例钉住，再分化就在这里红。
     */
    @Test
    public void mappingKeyIsUnifiedAcrossAllMappingRules() {
        List<OntologyActionRule> mappingRules = List.of( //
                new CreateObjectRule() //
                , new ModifyObjectRule() //
                , new FunctionBackedRule());
        for (OntologyActionRule rule : mappingRules) {
            Option mappings = pair(rule.getLiteria(), PropertyMappingConfig.KEY_MAPPINGS);
            Assert.assertNotNull(rule.getClass().getSimpleName() + " 的 literia 必须带 "
                    + PropertyMappingConfig.KEY_MAPPINGS + " 对", mappings);
            Assert.assertTrue(rule.getClass().getSimpleName() + " 未配置的映射行应描述成空列表",
                    ((List<?>) mappings.getValue()).isEmpty());
        }
    }

    /**
     * 无字段的来源（当前时间）也要有语义标签，否则该行键后头空空如也。
     */
    @Test
    public void currentTimeSourceHasSemanticTag() {
        PropertyMappingConfig mapping = new PropertyMappingConfig();
        mapping.target = "createdAt";
        mapping.source = new CurrentTimeValueSource();

        List<UDFDesc> rowLiteria = PropertyMappingConfig.literia(Collections.singletonList(mapping));
        Option targetPair = rowLiteria.get(0).getPairs().get(0);
        Assert.assertEquals("target 作行键", "createdAt", targetPair.getName());
        Option paramPair = pair(nested(targetPair), "currentTime");
        Assert.assertNotNull(paramPair);
        Assert.assertEquals("当前时间", paramPair.getValue());
    }

    /**
     * 通知渠道委托一层：渠道子类各自描述收件人字段。
     */
    @Test
    public void notificationRuleDelegatesChannel() {
        NotificationRule rule = new NotificationRule();
        EmailNotifyChannel channel = new EmailNotifyChannel();
        channel.emailAddress = "user@example.com";
        rule.notifyChannel = channel;

        List<UDFDesc> channelLiteria = nested(pair(rule.getLiteria(), "notifyChannel"));
        Option addressPair = pair(channelLiteria, "emailAddress");
        Assert.assertNotNull(addressPair);
        Assert.assertEquals("user@example.com", addressPair.getValue());
    }

    /**
     * Webhook 的请求头行（头名作键、头值未展开原样）与重试策略委托都要到位。
     */
    @Test
    public void webhookRuleDelegatesHeadersAndRetryPolicy() {
        WebhookRule rule = new WebhookRule();
        HttpHeaderConfig token = new HttpHeaderConfig();
        token.name = "X-Token";
        token.value = "${tokenParam}";
        HttpHeaderConfig env = new HttpHeaderConfig();
        env.name = "X-Env";
        env.value = "prod";
        rule.headers = List.of(token, env);
        rule.retryOnFailure = new AutoRetryPolicy();

        List<UDFDesc> headerLiteria = nested(pair(rule.getLiteria(), "headers"));
        Assert.assertEquals("两行请求头产出两条描述", 2, headerLiteria.size());
        Assert.assertEquals("${tokenParam}", pair(headerLiteria, "X-Token").getValue());
        Assert.assertEquals("prod", pair(headerLiteria, "X-Env").getValue());

        List<UDFDesc> retryLiteria = nested(pair(rule.getLiteria(), "retryOnFailure"));
        Assert.assertEquals("自动重试", pair(retryLiteria, "policy").getValue());
        Assert.assertEquals(String.valueOf(AutoRetryPolicy.MAX_RETRIES),
                pair(retryLiteria, "maxRetries").getValue());

        // off 档零字段实现同样要有语义标签
        WebhookRule noRetryRule = new WebhookRule();
        noRetryRule.retryOnFailure = new NoRetryPolicy();
        Assert.assertEquals("不重试",
                pair(nested(pair(noRetryRule.getLiteria(), "retryOnFailure")), "policy").getValue());
    }

    /**
     * 链接规则的源 / 目标 RID 参数对与可选的兼容字段（非空才下发）。
     */
    @Test
    public void linkRuleLiteriaContainsRidParams() {
        CreateLinkRule createLink = new CreateLinkRule();
        createLink.linkTypeName = "belongsTo";
        List<UDFDesc> literia = createLink.getLiteria();
        Assert.assertEquals("belongsTo", pair(literia, "linkTypeName").getValue());
        Assert.assertEquals("sourceRid", pair(literia, "sourceObjectRidParam").getValue());
        Assert.assertEquals("targetRid", pair(literia, "targetObjectRidParam").getValue());

        // 兼容字段留空时跳过该对
        Assert.assertNotNull("linkTypeName 留空时不应有该对",
                pair(new DeleteLinkRule().getLiteria(), "sourceObjectRidParam"));
        Assert.assertNull("linkTypeName 留空时不应有该对",
                pair(new DeleteLinkRule().getLiteria(), "linkTypeName"));
    }

    // ===================================================================
    // 4. 列表助手
    // ===================================================================

    /**
     * 两个静态列表助手对 null / 空入参都要返回空列表（不是 null）——
     * 规则侧拿到后直接放进 UDFDesc，返回 null 会让前端拿到非法载荷。
     */
    @Test
    public void listHelpersReturnEmptyOnNullOrBlank() {
        Assert.assertTrue(PropertyMappingConfig.literia(null).isEmpty());
        Assert.assertTrue(PropertyMappingConfig.literia(Collections.emptyList()).isEmpty());
        Assert.assertTrue(HttpHeaderConfig.literia(null).isEmpty());
        Assert.assertTrue(HttpHeaderConfig.literia(Collections.emptyList()).isEmpty());
    }

    // ===================================================================
    // 断言辅助
    // ===================================================================

    /**
     * 在 literia 的全部键值对里按键名找一对；找不到返回 null。
     */
    private static Option pair(List<UDFDesc> literia, String key) {
        for (UDFDesc desc : literia) {
            for (Option pair : desc.getPairs()) {
                if (key.equals(pair.getName())) {
                    return pair;
                }
            }
        }
        return null;
    }

    /**
     * 断言某对的值是嵌套 UDFDesc 列表并返回它——委托层级接错时在这里显式失败，
     * 而不是把 ClassCastException 推迟到前端渲染。
     */
    @SuppressWarnings("unchecked")
    private static List<UDFDesc> nested(Option pair) {
        Assert.assertTrue("键 " + pair.getName() + " 的值应是嵌套 UDFDesc 列表，实际 "
                + (pair.getValue() == null ? "null" : pair.getValue().getClass().getName()),
                pair.getValue() instanceof List);
        return (List<UDFDesc>) pair.getValue();
    }
}
