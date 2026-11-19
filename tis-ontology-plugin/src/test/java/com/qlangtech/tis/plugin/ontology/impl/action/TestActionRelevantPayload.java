package com.qlangtech.tis.plugin.ontology.impl.action;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.extension.OneStepOfMultiSteps;
import com.qlangtech.tis.plugin.ontology.OntologyAction;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.CreateLinkRule;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.CreateObjectRule;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.DeleteObjectRule;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.OntologyActionRule;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.PropertyMappingConfig;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.RuleGroup;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.RuleType;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.WebhookRule;
import com.qlangtech.tis.plugin.ontology.impl.action.type.LinkActionType;
import com.qlangtech.tis.plugin.ontology.impl.action.type.ObjectActionType;
import com.qlangtech.tis.plugin.ontology.impl.action.type.SideEffectActionType;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

/**
 * {@code getActionRelevant} 响应体里 {@code rules} / {@code submissionCriteria} 两段的回归测试。
 *
 * <p>这两段此前是<b>整段缺失</b>的：{@code rules} 键根本没人写（旧实现把收集逻辑注释掉了），
 * {@code submissionCriteria} 曾被硬写成 {@code []}。两者在前端都表现为「表格一直是空的」，
 * 没有任何报错 —— 所以必须用单测钉住形状。
 *
 * <p>本类全部走 {@code DefaultDesc} 上的包级静态方法，<b>不依赖 TIS 运行期</b>
 * （不需要 {@code --add-opens}，也不需要拉起扩展索引）：目标字段的取值刻意避开了
 * {@code getDescriptor()}，见 {@link #webhookRuleFallsBackToRuleGroupDisplayName()}。
 */
public class TestActionRelevantPayload {

    // ===================================================================
    // 1. rules —— 三条常用规则路径
    // ===================================================================

    /**
     * 对象规则：{@code ruleType} 必须是 {@link RuleType} 的<b>常量名</b>（前端用它判断危险操作），
     * {@code ruleTypeLabel} 是中文名，{@code target} 是对象类型引用。
     */
    @Test
    public void objectRuleBecomesOneRow() {
        CreateObjectRule rule = new CreateObjectRule();
        rule.propertyMappings = Arrays.asList(mapping("customerName"), mapping("amount"));

        JSONArray rows = DefaultOntologyAction.DefaultDesc.buildRules(List.of(
                objectAction(rule, "创建订单", "Order")));

        Assert.assertEquals("一条 ActionType 就是一行", 1, rows.size());
        JSONObject row = rows.getJSONObject(0);
        Assert.assertEquals("identity 字段名是前端行 key", "创建订单", row.getString("ruleName"));
        Assert.assertEquals("ruleType 必须是枚举常量名 —— 前端按 CREATE_LINK / DELETE_OBJECT 判危险操作，"
                + "换成中文标签就匹配不上了", RuleType.CREATE_OBJECT.name(), row.getString("ruleType"));
        Assert.assertEquals("ruleTypeLabel 供 Rules 表直接展示",
                RuleType.CREATE_OBJECT.getDisplayName(), row.getString("ruleTypeLabel"));
        Assert.assertEquals("对象规则的 target 是对象类型引用", "Order", row.getString("target"));
        Assert.assertEquals("properties 按映射行顺序列出目标字段，且自动生成 RID 时不列 rid",
                Arrays.asList("customerName", "amount"), strings(row.getJSONArray("properties")));
    }

    /**
     * {@code generateRid = false} 时用户必须自己提供 rid，这个字段也要出现在 properties 里，
     * 否则详情页看不出「这条规则需要 rid 参数」。
     */
    @Test
    public void createObjectRuleListsRidWhenNotGenerated() {
        CreateObjectRule rule = new CreateObjectRule();
        rule.generateRid = false;

        JSONArray rows = DefaultOntologyAction.DefaultDesc.buildRules(List.of(
                objectAction(rule, "手工建单", "Order")));

        Assert.assertEquals(List.of("rid"), strings(rows.getJSONObject(0).getJSONArray("properties")));
    }

    /**
     * 链接规则：{@code target} 取 Action 级的 {@code linkTypeReference}
     * （规则上的 {@code linkTypeName} 只是兼容字段，优先级更低）。
     */
    @Test
    public void linkRuleTargetIsLinkTypeReference() {
        LinkActionType actionType = new LinkActionType();
        actionType.ruleName = "挂载客户";
        actionType.linkTypeReference = "OrderCustomerLink";
        actionType.linkRule = new CreateLinkRule();

        JSONArray rows = DefaultOntologyAction.DefaultDesc.buildRules(List.of(actionType));
        JSONObject row = rows.getJSONObject(0);

        Assert.assertEquals(RuleType.CREATE_LINK.name(), row.getString("ruleType"));
        Assert.assertEquals(RuleType.CREATE_LINK.getDisplayName(), row.getString("ruleTypeLabel"));
        Assert.assertEquals("链接规则的 target 是 Action 级 linkTypeReference", "OrderCustomerLink",
                row.getString("target"));
        Assert.assertEquals("两个 RID 参数名都要列出", Arrays.asList("sourceRid", "targetRid"),
                strings(row.getJSONArray("properties")));
    }

    /**
     * 副作用规则（Webhook）没有目标对象类型，也没有参数引用：{@code target} 回退规则分组显示名，
     * {@code properties} 为空数组 —— 不能是 {@code null}，前端 {@code *ngFor} 会在 null 上炸。
     */
    @Test
    public void webhookRuleFallsBackToRuleGroupDisplayName() {
        SideEffectActionType actionType = new SideEffectActionType();
        actionType.ruleName = "通知下游";
        actionType.sideEffectRule = new WebhookRule();

        JSONArray rows = DefaultOntologyAction.DefaultDesc.buildRules(List.of(actionType));
        JSONObject row = rows.getJSONObject(0);

        Assert.assertEquals(RuleType.WEBHOOK.name(), row.getString("ruleType"));
        Assert.assertEquals(RuleType.WEBHOOK.getDisplayName(), row.getString("ruleTypeLabel"));
        Assert.assertEquals("没有统一目标字段时回退规则分组显示名（不是 ActionType descriptor —— "
                        + "后者要拉起 TIS 扩展索引，详情页不该依赖它）",
                RuleGroup.SIDE_EFFECT.getDisplayName(), row.getString("target"));
        Assert.assertNotNull("properties 必须是空数组而不是 null", row.getJSONArray("properties"));
        Assert.assertTrue(row.getJSONArray("properties").isEmpty());
    }

    /**
     * 半配置状态（选了规则分组、具体规则还没填）：不能抛异常，
     * {@code ruleType} / {@code ruleTypeLabel} 回退到 {@link RuleGroup}。
     */
    @Test
    public void ruleWithoutRuleInstanceFallsBackToRuleGroup() {
        ObjectActionType actionType = new ObjectActionType();
        actionType.ruleName = "半配置";
        actionType.objectTypeReference = "Order";
        // objectRule 保持 null

        JSONArray rows = DefaultOntologyAction.DefaultDesc.buildRules(List.of(actionType));
        JSONObject row = rows.getJSONObject(0);

        Assert.assertEquals(RuleGroup.ONTOLOGY_OBJECT.name(), row.getString("ruleType"));
        Assert.assertEquals(RuleGroup.ONTOLOGY_OBJECT.getDisplayName(), row.getString("ruleTypeLabel"));
        Assert.assertNotNull(row.getJSONArray("properties"));
        Assert.assertTrue("规则未配置时没有字段可列", row.getJSONArray("properties").isEmpty());
    }

    /**
     * 空列表 / 含 null 元素的列表：返回空数组而非抛 NPE。请求一个规则全空的 Action
     * 不该让详情页 500。
     */
    @Test
    public void emptyOrNullElementListsAreTolerated() {
        Assert.assertTrue(DefaultOntologyAction.DefaultDesc.buildRules(null).isEmpty());
        Assert.assertTrue(DefaultOntologyAction.DefaultDesc.buildRules(List.of()).isEmpty());
        Assert.assertTrue("null 元素跳过",
                DefaultOntologyAction.DefaultDesc.buildRules(Arrays.asList((com.qlangtech.tis.plugin.ontology.impl.action.type.ActionType) null))
                        .isEmpty());
    }

    /**
     * 概览页的 {@code objectType} 只认带对象类型引用的对象规则；纯副作用组合时为 {@code null}。
     */
    @Test
    public void resolveObjectTypeOnlyLooksAtObjectRules() {
        SideEffectActionType sideEffect = new SideEffectActionType();
        sideEffect.sideEffectRule = new WebhookRule();
        Assert.assertNull("没有对象规则时 objectType 为 null",
                DefaultOntologyAction.DefaultDesc.resolveObjectType(List.of(sideEffect)));

        Assert.assertEquals("取第一条对象规则的对象类型",
                "Order",
                DefaultOntologyAction.DefaultDesc.resolveObjectType(List.of(
                        objectAction(new CreateObjectRule(), "建单", "Order"))));
    }

    // ===================================================================
    // 2. submissionCriteria
    // ===================================================================

    /**
     * 五字段原样下发，枚举走 {@code name()}（中文由前端映射）。
     *
     * <p>一元操作符（{@code IS_NULL}）的 {@code value} 保持 {@code null}：前端据此渲染空值单元格，
     * 服务端不应自作主张填一个占位符。
     */
    @Test
    public void submissionCriteriaKeepEnumNamesAndNullValue() {
        SubmissionCriterion equals = criterion("金额下限", SubmissionCriterion.SubjectKind.OBJECT_PROPERTY,
                "amount", SubmissionCriterion.ComparisonOperator.GREATER_THAN, "100");
        SubmissionCriterion isNull = criterion("客户必填", SubmissionCriterion.SubjectKind.PARAMETER,
                "customerName", SubmissionCriterion.ComparisonOperator.IS_NULL, null);

        JSONArray rows = DefaultOntologyAction.DefaultDesc.buildSubmissionCriteria(Arrays.asList(equals, isNull));
        Assert.assertEquals(2, rows.size());

        JSONObject first = rows.getJSONObject(0);
        Assert.assertEquals("金额下限", first.getString("label"));
        Assert.assertEquals("subjectKind 走枚举名", "OBJECT_PROPERTY", first.getString("subjectKind"));
        Assert.assertEquals("amount", first.getString("subjectRef"));
        Assert.assertEquals("operator 走枚举名", "GREATER_THAN", first.getString("operator"));
        Assert.assertEquals("100", first.getString("value"));

        JSONObject second = rows.getJSONObject(1);
        Assert.assertEquals("PARAMETER", second.getString("subjectKind"));
        Assert.assertEquals("IS_NULL", second.getString("operator"));
        Assert.assertNull("一元操作符不需要比较值", second.getString("value"));
    }

    /**
     * 残缺条件（手工改过 XML / 老数据把枚举写成 null）时整行仍要下发，只是对应字段为 null ——
     * 少一行会让用户以为自己配置的条件丢了。
     *
     * <p>注意两个枚举字段都有字段初始值（{@code OBJECT_PROPERTY} / {@code EQUALS}），
     * 所以「新建的实例」不会是 null；这里显式置 null 才模拟得出老数据。
     */
    @Test
    public void submissionCriterionWithNullEnumsStillEmitsRow() {
        SubmissionCriterion broken = new SubmissionCriterion();
        broken.label = "残缺条件";
        broken.subjectKind = null;
        broken.operator = null;

        JSONArray rows = DefaultOntologyAction.DefaultDesc.buildSubmissionCriteria(List.of(broken));
        Assert.assertEquals(1, rows.size());
        Assert.assertEquals("残缺条件", rows.getJSONObject(0).getString("label"));
        Assert.assertNull(rows.getJSONObject(0).getString("subjectKind"));
        Assert.assertNull(rows.getJSONObject(0).getString("operator"));
    }

    /**
     * 新建实例的两个枚举字段必须带默认值：向导里新增一行时如果两项都空着，
     * 用户得先猜「左侧字段来自哪里、默认怎么比」。
     */
    @Test
    public void submissionCriterionDefaultsAreObjectPropertyAndEquals() {
        JSONArray rows = DefaultOntologyAction.DefaultDesc.buildSubmissionCriteria(
                List.of(new SubmissionCriterion()));

        Assert.assertEquals(SubmissionCriterion.SubjectKind.OBJECT_PROPERTY.name(),
                rows.getJSONObject(0).getString("subjectKind"));
        Assert.assertEquals(SubmissionCriterion.ComparisonOperator.EQUALS.name(),
                rows.getJSONObject(0).getString("operator"));
    }

    @Test
    public void submissionCriteriaNullListYieldsEmptyArray() {
        Assert.assertTrue(DefaultOntologyAction.DefaultDesc.buildSubmissionCriteria(null).isEmpty());
    }

    // ===================================================================
    // 3. 存量两步 XML 的优雅降级
    // ===================================================================

    /**
     * 存量 Action 只有 2 个步骤槽位。{@code getSubmissionCriteria()} 取第 4 槽时
     * 必须返回空列表，<b>不能</b> {@code ArrayIndexOutOfBoundsException} ——
     * 后者会让整个详情页打不开。
     */
    @Test
    public void submissionCriteriaDegradesOnLegacyTwoStepLayout() throws Exception {
        DefaultOntologyAction action = new DefaultOntologyAction();
        setSteps(action, new OneStepOfMultiSteps[]{metadata("legacy"), new ActionRuleSetter()});

        Assert.assertTrue("存量两步 Action 没有提交条件（== 不限制提交）",
                action.getSubmissionCriteria().isEmpty());
    }

    /**
     * {@code readResolve()} 把槽位补齐到声明长度：前端编辑模式按下标回放全部步骤、
     * {@code setSteps} 又断言长度与声明一致，缺了会在打开编辑页时直接失败。
     */
    @Test
    public void readResolveFillsLegacyStepsToDeclaredLength() throws Exception {
        DefaultOntologyAction action = new DefaultOntologyAction();
        setSteps(action, new OneStepOfMultiSteps[]{metadata("legacy"), new ActionRuleSetter()});

        Method readResolve = DefaultOntologyAction.class.getDeclaredMethod("readResolve");
        readResolve.setAccessible(true);
        Assert.assertSame("readResolve 必须返回 this，否则 XStream 会把实例换成 null", action,
                readResolve.invoke(action));

        OneStepOfMultiSteps[] steps = action.getMultiStepsSavedItems();
        Assert.assertEquals("补齐到声明长度",
                new DefaultOntologyAction.DefaultDesc().getStepDescriptionList().size(), steps.length);
        Assert.assertTrue("第 3 步补 ActionParameters", steps[2] instanceof ActionParameters);
        Assert.assertTrue("第 4 步补 ActionSubmissionCriteria", steps[3] instanceof ActionSubmissionCriteria);
        Assert.assertTrue("补齐后提交条件仍是空列表", action.getSubmissionCriteria().isEmpty());
        Assert.assertEquals("第一步原样保留", "legacy", action.getName());
    }

    /**
     * 已是 4 步的 Action 不能被 {@code readResolve()} 重建 —— 那会把用户填好的
     * 参数池 / 提交条件悄悄换成空实例。
     */
    @Test
    public void readResolveLeavesCompleteStepsUntouched() throws Exception {
        DefaultOntologyAction action = new DefaultOntologyAction();
        ActionParameters params = new ActionParameters();
        ActionSubmissionCriteria criteria = new ActionSubmissionCriteria();
        criteria.criteria = List.of(criterion("金额下限", SubmissionCriterion.SubjectKind.OBJECT_PROPERTY,
                "amount", SubmissionCriterion.ComparisonOperator.GREATER_THAN, "100"));
        OneStepOfMultiSteps[] original = {metadata("full"), new ActionRuleSetter(), params, criteria};
        setSteps(action, original);

        Method readResolve = DefaultOntologyAction.class.getDeclaredMethod("readResolve");
        readResolve.setAccessible(true);
        readResolve.invoke(action);

        Assert.assertSame("数组不能被替换", original, action.getMultiStepsSavedItems());
        Assert.assertEquals(1, action.getSubmissionCriteria().size());
    }

    // ===================================================================
    //  helpers
    // ===================================================================

    private static ObjectActionType objectAction(OntologyActionRule rule, String ruleName, String objectType) {
        ObjectActionType actionType = new ObjectActionType();
        actionType.ruleName = ruleName;
        actionType.objectRule = rule;
        actionType.objectTypeReference = objectType;
        return actionType;
    }

    private static PropertyMappingConfig mapping(String target) {
        PropertyMappingConfig config = new PropertyMappingConfig();
        config.target = target;
        return config;
    }

    private static SubmissionCriterion criterion(String label, SubmissionCriterion.SubjectKind kind,
                                                 String subjectRef,
                                                 SubmissionCriterion.ComparisonOperator operator, String value) {
        SubmissionCriterion criterion = new SubmissionCriterion();
        criterion.label = label;
        criterion.subjectKind = kind;
        criterion.subjectRef = subjectRef;
        criterion.operator = operator;
        criterion.value = value;
        return criterion;
    }

    private static ActionMetadata metadata(String name) {
        ActionMetadata metadata = new ActionMetadata();
        metadata.name = name;
        metadata.displayName = name + "-display";
        metadata.description = name + "-desc";
        return metadata;
    }

    private static void setSteps(DefaultOntologyAction action, OneStepOfMultiSteps[] steps) throws Exception {
        Field field = OntologyAction.class.getDeclaredField("stepsPlugin");
        field.setAccessible(true);
        field.set(action, steps);
    }

    private static List<String> strings(JSONArray array) {
        Assert.assertNotNull("properties 不能为 null", array);
        List<String> result = new java.util.ArrayList<>(array.size());
        for (Object o : array) {
            result.add(String.valueOf(o));
        }
        return result;
    }
}
