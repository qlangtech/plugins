package com.qlangtech.tis.plugin.ontology.impl.action.type;

import com.qlangtech.tis.plugin.annotation.ValidateRule;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.OntologyObjectType;
import com.qlangtech.tis.plugin.ontology.OntologyProperty;
import com.qlangtech.tis.plugin.ontology.OntologyType;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.CreateObjectRule;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.CurrentTimeValueSource;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.DeleteObjectRule;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.FromParameterValueSource;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.LiteralValueSource;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.ModifyObjectRule;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.PropertyMappingConfig;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 一个 Action 组合多个<b>不同对象类型</b>的规则时，参数池 / 运行时表单的命名空间契约。
 *
 * <p>参数名同时是表单字段名与 SQL 列名（见 {@code ActionFormDescriptorBuilder}、
 * {@code ObjectActionType#getPluginPropertyVals}）。两个对象类型都有 {@code id} 时：
 * <ul>
 *   <li>参数池按 identity 去重，后一条规则的 {@code id} 被静默合并掉；</li>
 *   <li>整张表单被摊平成一份值，执行时拿 {@code id} 同时当两张表的列名。</li>
 * </ul>
 * 因此多对象类型时参数名带 {@code <objectTypeReference>_} 前缀，单对象类型保持原样。
 */
public class TestMultiObjectTypeNamespace {

    /**
     * 只有一个对象类型（或没有对象规则）时不加前缀：存量 Action 的参数池、生成的 groovy、
     * 落盘文件必须逐字节不变。
     */
    @Test
    public void singleObjectTypeKeepsPlainNames() {
        ObjectActionType userCreate = objectRule("createUser", "user");
        ObjectActionType userModify = objectRule("modifyUser", "user");

        Assert.assertEquals(List.of("user"),
                ObjectActionType.objectTypeReferences(List.of(userCreate, userModify)));
        Assert.assertEquals("同一对象类型的两条规则仍算单对象类型，不得加前缀",
                "", userCreate.namespacePrefix(List.of("user")));
        Assert.assertEquals("", userModify.namespacePrefix(List.of("user")));

        Assert.assertEquals("没有任何对象规则", List.of(), ObjectActionType.objectTypeReferences(List.of()));
        Assert.assertEquals("", ObjectActionType.namespacePrefix("user", List.of()));
        Assert.assertEquals("", ObjectActionType.namespacePrefix("user", null));
    }

    /**
     * 多于一个对象类型时每条规则都拿到自己的前缀，且对象类型列表去重保序。
     */
    @Test
    public void multipleObjectTypesGetNamespaced() {
        ObjectActionType userCreate = objectRule("createUser", "user");
        ObjectActionType orderModify = objectRule("modifyOrder", "order_detail");
        ObjectActionType userModify = objectRule("modifyUser", "user");

        List<String> refs = ObjectActionType.objectTypeReferences(List.of(userCreate, orderModify, userModify));
        Assert.assertEquals("对象类型引用必须去重且保持出现顺序", List.of("user", "order_detail"), refs);

        Assert.assertEquals("user_", userCreate.namespacePrefix(refs));
        Assert.assertEquals("order_detail_", orderModify.namespacePrefix(refs));
        Assert.assertEquals("同一对象类型的多条规则前缀相同", userCreate.namespacePrefix(refs),
                userModify.namespacePrefix(refs));
    }

    /**
     * 对象类型名可以含中文 / 连字符，而前缀要与属性名拼成 groovy 字段名，
     * 必须满足 {@link Validator#identity_strict}。
     */
    @Test
    public void prefixIsValidIdentifier() {
        Assert.assertEquals("order_detail_", ObjectActionType.qualifyPrefix("order_detail"));
        Assert.assertEquals("order_detail_", ObjectActionType.qualifyPrefix("order-detail"));
        Assert.assertEquals("order_detail_", ObjectActionType.qualifyPrefix("order detail"));
        Assert.assertEquals("_2024_orders_", ObjectActionType.qualifyPrefix("2024-orders"));

        // identity_strict = [A-Z\da-z_]+，且不能以数字开头
        String paramId = ObjectActionType.qualifyPrefix("订单-明细") + "id";
        Assert.assertTrue("中文对象类型名不能拼出非法字段名：" + paramId,
                paramId.matches("[_A-Za-z][_A-Za-z0-9]*"));
        for (ValidateRule rule : Validator.identity_strict.rules) {
            Assert.assertTrue("参数名 " + paramId + " 必须通过 identity_strict，失败规则：" + rule,
                    rule.isValid(paramId));
        }
    }

    /**
     * 未带命名空间上下文（单对象类型）时执行期读到的是全部表单字段 —— 与改造前一致。
     */
    @Test
    public void scopedValuesWithoutNamespaceKeepsAllFields() {
        ObjectActionType rule = objectRule("createUser", "user");
        Map<String, Object> allValues = new LinkedHashMap<>();
        allValues.put("id", 1);
        allValues.put("name", "baisui");
        allValues.put("deleteConfirmed", null);

        Map<String, Object> scoped = rule.scopeValues(allValues, Set.of("id", "name"), List.of());

        Assert.assertEquals("单对象类型不得过滤任何字段（含 deleteConfirmed 的存量行为）",
                allValues, scoped);
    }

    /**
     * 多对象类型时只保留本规则命名空间下的、且对象类型真实拥有的属性，并去掉前缀。
     */
    @Test
    public void scopedValuesWithNamespaceStripsPrefixAndDropsForeignFields() {
        ObjectActionType rule = objectRule("modifyOrder", "order_detail");
        Map<String, Object> allValues = new LinkedHashMap<>();
        allValues.put("user_id", 1);
        allValues.put("user_name", "baisui");
        allValues.put("order_detail_id", 99);
        allValues.put("order_detail_amount", 12.5);
        allValues.put("sourceRidParam", "x");
        allValues.put("deleteConfirmed", null);

        Map<String, Object> scoped = rule.scopeValues(allValues,
                Set.of("id", "amount"), List.of("user", "order_detail"));

        Assert.assertEquals("只应保留本对象类型的两个属性：" + scoped, 2, scoped.size());
        Assert.assertEquals(99, scoped.get("id"));
        Assert.assertEquals(12.5, scoped.get("amount"));
        Assert.assertFalse("别的对象类型 / 非列参数不得进 INSERT / UPDATE：" + scoped,
                scoped.containsKey("user_id") || scoped.containsKey("sourceRidParam")
                        || scoped.containsKey("deleteConfirmed"));
    }

    /**
     * 身份参数名同样要带前缀，否则前端预填写进 {@code ridFieldName} 后找不到表单字段。
     */
    @Test
    public void identityParameterNameCarriesNamespace() {
        // 未声明主键的对象类型：身份参数名回落到规则上的 ridParameterName
        com.qlangtech.tis.plugin.ontology.OntologyObjectType noPk =
                org.mockito.Mockito.mock(com.qlangtech.tis.plugin.ontology.OntologyObjectType.class);
        org.mockito.Mockito.when(noPk.hasDisablePK()).thenReturn(false);
        org.mockito.Mockito.when(noPk.getPk()).thenReturn(java.util.Optional.empty());
        Assert.assertEquals("rid", ObjectActionType.resolveIdentityParameterName(noPk, "rid", ""));
        Assert.assertEquals("user_rid", ObjectActionType.resolveIdentityParameterName(noPk, "rid", "user_"));
    }

    // ==================== propertyMappings fill-gap（2026/11/24 映射通电） ====================

    /**
     * 映射行只补本命名空间缺失的键：副表共享字段（id/amount）从主表命名空间参数池取值。
     * 本 bug 钉子——执行链此前从不消费 {@code propertyMappings}，副表 INSERT 缺共享列。
     */
    @Test
    public void mappedValuesFillMissingNamespaceFields() {
        ObjectActionType rule = objectRule("createOrderDetail", "order_detail");
        CreateObjectRule createRule = (CreateObjectRule) rule.objectRule;
        createRule.propertyMappings = List.of(
                mapping("id", "order_id"), mapping("amount", "order_amount"));

        Map<String, Object> allValues = new LinkedHashMap<>();
        allValues.put("order_id", 99);
        allValues.put("order_amount", 12.5);
        allValues.put("detail_owner_email", "baisui");

        Map<String, Object> scoped = new LinkedHashMap<>();
        scoped.put("owner_email", "baisui");
        scoped.put("remark", "ok");

        Map<String, Object> merged = ObjectActionType.mergePropertyMappings(
                rule.objectRule, scoped, allValues,
                Map.of("id", OntologyType.LONG, "amount", OntologyType.DECIMAL,
                        "owner_email", OntologyType.STRING, "remark", OntologyType.STRING));

        Assert.assertEquals("表单已有字段保持原值", "baisui", merged.get("owner_email"));
        Assert.assertEquals("跨命名空间映射补入共享字段的值", 99, merged.get("id"));
        Assert.assertEquals(12.5, merged.get("amount"));
        Assert.assertEquals("只补缺口，不得多键", 4, merged.size());
    }

    /**
     * fill-gap 优先级钉子：表单值优先，{@code CurrentTimeValueSource} 映射行不得冲掉用户填的日期
     * （存量配置里主表日期既是必填表单参数又有当前时间映射行）。
     */
    @Test
    public void formValuesWinOverMappings() {
        ObjectActionType rule = objectRule("createMain", "order");
        CreateObjectRule createRule = (CreateObjectRule) rule.objectRule;
        PropertyMappingConfig dateRow = new PropertyMappingConfig();
        dateRow.target = "report_date";
        dateRow.source = new CurrentTimeValueSource();
        createRule.propertyMappings = List.of(dateRow);

        Map<String, Object> allValues = new LinkedHashMap<>();
        allValues.put("report_date", "2026-11-24");
        Map<String, Object> scoped = new LinkedHashMap<>();
        scoped.put("report_date", "2026-11-24");

        Map<String, Object> merged = ObjectActionType.mergePropertyMappings(
                rule.objectRule, scoped, allValues,
                Map.of("report_id", OntologyType.STRING, "report_date", OntologyType.DATE));

        Assert.assertEquals("表单已填的日期不得被映射行冲成当前时间", "2026-11-24", merged.get("report_date"));
    }

    /** 参数解析为 null 的映射行跳过（{@code buildMappedValues} null-skip 传导）。 */
    @Test
    public void mappingWithMissingParameterStaysSkipped() {
        ObjectActionType rule = objectRule("createOrderDetail", "order_detail");
        ((CreateObjectRule) rule.objectRule).propertyMappings =
                List.of(mapping("id", "order_id")); // 裸名不在带命名空间前缀的参数池里

        Map<String, Object> allValues = new LinkedHashMap<>();
        allValues.put("detail_owner_email", "x");
        Map<String, Object> scoped = new LinkedHashMap<>();
        scoped.put("owner_email", "x");

        Map<String, Object> merged = ObjectActionType.mergePropertyMappings(
                rule.objectRule, scoped, allValues,
                Map.of("id", OntologyType.LONG, "owner_email", OntologyType.STRING));

        Assert.assertEquals("缺失参数不产生键", 1, merged.size());
    }

    /** target 越界的陈旧映射行丢弃，防止拼出未知列的 SQL。 */
    @Test
    public void mappingTargetOutsideColsDropped() {
        ObjectActionType rule = objectRule("createOrderDetail", "order_detail");
        ((CreateObjectRule) rule.objectRule).propertyMappings = List.of(
                mapping("deleted_prop", "order_id"), mapping("id", "order_id"));

        Map<String, Object> allValues = new LinkedHashMap<>();
        allValues.put("order_id", 7);
        Map<String, Object> scoped = new LinkedHashMap<>();

        Map<String, Object> merged = ObjectActionType.mergePropertyMappings(
                rule.objectRule, scoped, allValues, Map.of("id", OntologyType.LONG));

        Assert.assertEquals("越界 target 丢弃、合法 target 补入", 1, merged.size());
        Assert.assertEquals(7, merged.get("id"));
    }

    /** Delete 规则无 {@code propertyMappings} 字段原样返回；单对象类型下字面量来源同样按 fill-gap 补键。 */
    @Test
    public void deleteRuleUntouchedAndLiteralSourceFillsOnSingleNamespace() {
        ObjectActionType deleteRule = objectRule("deleteOrderDetail", "order_detail");
        deleteRule.objectRule = new DeleteObjectRule();
        Map<String, Object> scoped = new LinkedHashMap<>();
        scoped.put("rid", 1);

        Assert.assertSame("Delete 规则无映射，map 原样返回", scoped,
                ObjectActionType.mergePropertyMappings(
                        deleteRule.objectRule, scoped, new LinkedHashMap<>(), Map.of("rid", OntologyType.LONG)));

        ObjectActionType userRule = objectRule("createUser", "user");
        CreateObjectRule createUser = (CreateObjectRule) userRule.objectRule;
        LiteralValueSource literal = new LiteralValueSource();
        literal.literalValue = "system";
        PropertyMappingConfig row = new PropertyMappingConfig();
        row.target = "created_by";
        row.source = literal;
        createUser.propertyMappings = List.of(row);

        Map<String, Object> merged = ObjectActionType.mergePropertyMappings(
                userRule.objectRule, new LinkedHashMap<>(), new LinkedHashMap<>(),
                Map.of("created_by", OntologyType.STRING));
        Assert.assertEquals("单对象类型下固定字面量映射生效", "system", merged.get("created_by"));
    }

    /**
     * canonical 参数解析：共享属性归首个贡献的规则；单对象类型退化为裸属性名。
     * 参数池候选标注与映射默认行预置共用这一处事实源（C0）。
     */
    @Test
    public void canonicalParamsFirstContributorWins() {
        ObjectActionType mainRule = Mockito.spy(objectRule("createMain", "chartxy_daily_report"));
        Mockito.doReturn(objectTypeWith("report_id", "report_date", "department"))
                .when(mainRule).loadObjectType();
        ObjectActionType detailRule = Mockito.spy(objectRule("createDetail", "chartxy_daily_report_detail"));
        Mockito.doReturn(objectTypeWith("report_id", "report_date", "owner_email"))
                .when(detailRule).loadObjectType();

        Map<String, String> canonical = ObjectActionType.resolveCanonicalParams(
                List.of(mainRule, detailRule),
                ObjectActionType.objectTypeReferences(List.of(mainRule, detailRule)));

        Assert.assertEquals("共享属性归首个贡献规则（主表前缀）",
                "chartxy_daily_report_report_id", canonical.get("report_id"));
        Assert.assertEquals("chartxy_daily_report_report_date", canonical.get("report_date"));
        Assert.assertEquals("副表独有属性用副表前缀",
                "chartxy_daily_report_detail_owner_email", canonical.get("owner_email"));
        Assert.assertEquals("主表独有属性", "chartxy_daily_report_department", canonical.get("department"));
    }

    /** null 规则与半配置规则（属性加载失败）跳过，其余规则照常参与。 */
    @Test
    public void canonicalParamsToleratesHalfConfiguredRules() {
        ObjectActionType broken = Mockito.spy(objectRule("brokenRule", "ghost_type"));
        Mockito.doThrow(new IllegalStateException("object type not found"))
                .when(broken).loadObjectType();
        ObjectActionType good = Mockito.spy(objectRule("goodRule", "user"));
        Mockito.doReturn(objectTypeWith("name")).when(good).loadObjectType();

        Map<String, String> canonical = ObjectActionType.resolveCanonicalParams(
                Arrays.asList(null, broken, good), List.of("user"));

        Assert.assertEquals("null 与半配置规则跳过，其余照常", "name", canonical.get("name"));
        Assert.assertTrue("无规则输入返回空 map",
                ObjectActionType.resolveCanonicalParams(null, null).isEmpty());
    }

    // ==================== C4：执行前覆盖校验（typo 硬拦截 + CREATE 未覆盖只告警） ====================

    /**
     * C4 硬拦截钉子：映射行 parameterRef 在运行期表单值里解析不到 → IllegalStateException
     * 点名 {@code target ← ref}——参数池手工时代写错前缀一位就静默落 null、INSERT 缺列的 typo 面。
     */
    @Test
    public void unresolvableMappingRefFailsHardNamingTargetAndRef() {
        ObjectActionType rule = objectRule("createOrderDetail", "order_detail");
        ((CreateObjectRule) rule.objectRule).propertyMappings =
                List.of(mapping("id", "order_gost_id")); // 前缀写错一位的典型 typo

        Map<String, Object> allValues = new LinkedHashMap<>();
        allValues.put("order_id", 7);
        allValues.put("detail_owner_email", "x");

        try {
            ObjectActionType.validatePropertyCoverage(
                    List.of(rule), allValues, List.of("order", "order_detail"));
            Assert.fail("解析不到的映射 ref 必须硬拦截");
        } catch (IllegalStateException failure) {
            Assert.assertTrue("报错必须点名 target 与 typo ref：" + failure.getMessage(),
                    failure.getMessage().contains("id <- order_gost_id"));
            Assert.assertTrue("报错必须点名对象类型：" + failure.getMessage(),
                    failure.getMessage().contains("order_detail"));
        }
    }

    /**
     * 引用全部可解析时不抛；CREATE 规则存在未覆盖属性只 WARN 不阻断
     * （部分列插入是合法业务形态——副表先落主键列、其余列由后续流程补齐）。
     */
    @Test
    public void resolvableRefsPassAndUncoveredCreateColumnsOnlyWarn() {
        ObjectActionType rule = Mockito.spy(objectRule("createOrderDetail", "order_detail"));
        Mockito.doReturn(objectTypeWith("id", "amount", "memo")).when(rule).loadObjectType();
        ((CreateObjectRule) rule.objectRule).propertyMappings = List.of(mapping("id", "order_id"));

        Map<String, Object> allValues = new LinkedHashMap<>();
        allValues.put("order_id", 7);
        allValues.put("detail_amount", 1.5); // memo 无表单字段、无映射行 → 未覆盖，只 WARN

        ObjectActionType.validatePropertyCoverage(
                List.of(rule), allValues, List.of("order", "order_detail"));
    }

    /**
     * Modify 规则同样参与 typo 拦截：SET 子句的映射行引用错参数同样静默失效。
     */
    @Test
    public void modifyRuleTypoRefAlsoFailsHard() {
        ObjectActionType rule = objectRule("modifyOrderDetail", "order_detail");
        rule.objectRule = new ModifyObjectRule();
        ((ModifyObjectRule) rule.objectRule).propertyMappings =
                List.of(mapping("amount", "order_amount_typo"));

        Map<String, Object> allValues = new LinkedHashMap<>();
        allValues.put("detail_amount", 1.5);

        try {
            ObjectActionType.validatePropertyCoverage(
                    List.of(rule), allValues, List.of("order", "order_detail"));
            Assert.fail("Modify 的 typo ref 同样必须拦截");
        } catch (IllegalStateException expected) {
            Assert.assertTrue(expected.getMessage().contains("amount <- order_amount_typo"));
        }
    }

    /**
     * Delete / 非对象规则（含 null 条目）与 null 输入不参与校验——
     * {@code TestMultiStepActionTransaction} 的 mock ActionType 天然跳过，存量事务契约不受影响。
     */
    @Test
    public void deleteAndNonObjectRulesAndNullInputTolerated() {
        ObjectActionType deleteRule = objectRule("deleteOrder", "order");
        deleteRule.objectRule = new DeleteObjectRule();

        ObjectActionType.validatePropertyCoverage(
                Arrays.asList(deleteRule, Mockito.mock(ActionType.class), null),
                new LinkedHashMap<>(), List.of("order"));
        // null 输入兜底（第二个 null 显式按 Map 重载解析）
        ObjectActionType.validatePropertyCoverage(null, (Map<String, Object>) null, null);
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

    private static PropertyMappingConfig mapping(String target, String parameterRef) {
        PropertyMappingConfig row = new PropertyMappingConfig();
        row.target = target;
        row.source = new FromParameterValueSource(parameterRef);
        return row;
    }

    private static ObjectActionType objectRule(String ruleName, String objectType) {
        ObjectActionType actionType = new ObjectActionType();
        actionType.ruleName = ruleName;
        actionType.objectTypeReference = objectType;
        actionType.objectRule = new CreateObjectRule();
        return actionType;
    }
}