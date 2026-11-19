package com.qlangtech.tis.plugin.ontology.impl.action.type;

import com.qlangtech.tis.plugin.annotation.ValidateRule;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.CreateObjectRule;
import org.junit.Assert;
import org.junit.Test;

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

    private static ObjectActionType objectRule(String ruleName, String objectType) {
        ObjectActionType actionType = new ObjectActionType();
        actionType.ruleName = ruleName;
        actionType.objectTypeReference = objectType;
        actionType.objectRule = new CreateObjectRule();
        return actionType;
    }
}