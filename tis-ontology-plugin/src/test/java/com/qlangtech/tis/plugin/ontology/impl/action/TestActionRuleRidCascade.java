package com.qlangtech.tis.plugin.ontology.impl.action;

import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.ValueChangePipe;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.CreateLinkRule;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.DeleteLinkRule;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.DeleteObjectRule;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.ModifyObjectRule;
import com.qlangtech.tis.plugin.ontology.impl.action.type.LinkActionType;
import com.qlangtech.tis.plugin.ontology.impl.action.type.ObjectActionType;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

/**
 * RID 参数名与宿主 Action 主选择器之间的级联契约。
 *
 * <p>这条链路有三处独立约定，任何一处漏改都是静默失效（表单照常打开、候选恒空）：
 * <ol>
 *   <li>宿主 {@code ObjectActionType} / {@code LinkActionType} 的 descriptor 必须登记
 *       {@link ValueChangePipe}，且 toField 的<b>书写形态</b>（2 段路径 vs 数组顺序）必须与前端
 *       {@code PluginAction#doRenderValueChange} 回填时使用的键逐字一致；</li>
 *   <li>落点字段必须是 {@link FormFieldType#SELECTABLE} —— 管道只换候选，{@code INPUTTEXT}
 *       控件上根本没有渲染候选的位置（{@code SeriesConfig#segmentBy} 的现成对照）；</li>
 *   <li>落点字段的 descriptor 必须 {@code registerSelectOptions}，否则
 *       {@code DescriptorsJSON#getSelectOptions} 直接抛「has not been register」，整张表单打不开。</li>
 * </ol>
 */
public class TestActionRuleRidCascade {

    private static final String OBJECT_RULE_RID_FIELD = "objectRule.ridParameterName";
    private static final String LINK_RULE_SOURCE_RID_FIELD = "linkRule.sourceObjectRidParam";
    private static final String LINK_RULE_TARGET_RID_FIELD = "linkRule.targetObjectRidParam";

    /**
     * 对象操作：{@code objectTypeReference} → {@code objectRule.ridParameterName}。
     *
     * <p>必须是 2 段路径：{@code objectRule} 是普通可描述字段（不是 type-13 列表），
     * 而且 {@code RootFormProperties} 只把标量字段当 pipe source，规则描述对象本身永远不能做 fromField。
     */
    @Test
    public void objectTypeReferenceCascadesToObjectRuleRidParam() {
        Descriptor<?> descriptor = new ObjectActionType.DefaultDescriptor();

        ValueChangePipe pipe = descriptor.getValueChangePipe(ObjectActionType.KEY_OBJECT_TYPE_REFERENCE, false);
        Assert.assertNotNull("ObjectActionType 必须登记 objectTypeReference 的级联管道："
                + "否则 RID 参数名永远只能手打，写错只在执行期报 can not find parameter", pipe);
        Assert.assertEquals(ObjectActionType.KEY_OBJECT_TYPE_REFERENCE, pipe.getFromField());
        Assert.assertEquals("toField 必须恰好是该 2 段路径",
                List.of(OBJECT_RULE_RID_FIELD), Arrays.asList(pipe.getToField()));

        // 前端按 toField 反查 fromField 来挂 PipeAnchor，查不到就是静默空下拉
        Assert.assertTrue("getValueChangeFromFieldKeys 必须能由落点反查到 objectTypeReference",
                descriptor.getValueChangeFromFieldKeys(OBJECT_RULE_RID_FIELD)
                        .contains(ObjectActionType.KEY_OBJECT_TYPE_REFERENCE));
    }

    /**
     * 链接操作：{@code linkTypeReference} → 源 / 目标两个 RID 参数名。
     *
     * <p>toField 的数组顺序被 {@code Descriptor#getValueChangeFromFieldKeys} 的
     * {@code Arrays.binarySearch} 依赖，乱序会静默查不到（不抛异常、候选恒空）。
     */
    @Test
    public void linkTypeReferenceCascadesToBothRidParamsInLexicographicOrder() {
        Descriptor<?> descriptor = new LinkActionType.DefaultDescriptor();

        ValueChangePipe pipe = descriptor.getValueChangePipe("linkTypeReference", false);
        Assert.assertNotNull("LinkActionType 必须登记 linkTypeReference 的级联管道", pipe);
        Assert.assertEquals("linkTypeReference", pipe.getFromField());

        String[] toFields = pipe.getToField();
        Assert.assertEquals("链接规则有源 / 目标两个 RID 参数名，必须都登记",
                List.of(LINK_RULE_SOURCE_RID_FIELD, LINK_RULE_TARGET_RID_FIELD), Arrays.asList(toFields));

        String[] sorted = toFields.clone();
        Arrays.sort(sorted);
        Assert.assertArrayEquals("toField 必须字典序：Descriptor#getValueChangeFromFieldKeys 用 binarySearch 查找",
                sorted, toFields);

        for (String field : toFields) {
            Assert.assertTrue("由落点 " + field + " 反查不到 linkTypeReference",
                    descriptor.getValueChangeFromFieldKeys(field).contains("linkTypeReference"));
        }
    }

    /**
     * 五个落点字段必须是 SELECTABLE。
     */
    @Test
    public void ridParamFieldsAreSelectable() {
        assertSelectable(ModifyObjectRule.class, "ridParameterName");
        assertSelectable(DeleteObjectRule.class, "ridParameterName");
        assertSelectable(CreateLinkRule.class, "sourceObjectRidParam");
        assertSelectable(CreateLinkRule.class, "targetObjectRidParam");
        assertSelectable(DeleteLinkRule.class, "sourceObjectRidParam");
        assertSelectable(DeleteLinkRule.class, "targetObjectRidParam");
    }

    /**
     * 每个 SELECTABLE 落点都必须在自己的 descriptor 上登记候选来源，
     * 否则服务端序列化表单时直接抛异常，整个规则对话框打不开。
     * 这里登记的只是空列表兜底，真实候选由宿主 ActionType 的管道在渲染期回填。
     */
    @Test
    public void ridParamSelectOptionsAreRegistered() {
        assertSelectOptionsRegistered(new ModifyObjectRule.DefaultDescriptor(), "ridParameterName");
        assertSelectOptionsRegistered(new DeleteObjectRule.DefaultDescriptor(), "ridParameterName");
        assertSelectOptionsRegistered(new CreateLinkRule.DefaultDescriptor(), "sourceObjectRidParam");
        assertSelectOptionsRegistered(new CreateLinkRule.DefaultDescriptor(), "targetObjectRidParam");
        assertSelectOptionsRegistered(new DeleteLinkRule.DefaultDescriptor(), "sourceObjectRidParam");
        assertSelectOptionsRegistered(new DeleteLinkRule.DefaultDescriptor(), "targetObjectRidParam");

        Assert.assertNull("兜底候选应为空列表（不是 null）：null 会让 PluginAction 的 Collectors.toMap NPE",
                new ModifyObjectRule.DefaultDescriptor().getSelectOptions("ridParameterName").stream()
                        .findAny().orElse(null));
    }

    /**
     * 前端 type-6（SELECTABLE）分支只在 eprops 里带 {@code allow_custom_input} 时才给手输入口。
     * 这些字段的默认值是<b>参数名</b>（rid / sourceRid / targetRid），几乎不可能命中对象属性候选，
     * 不打开这个开关时已存值会渲染成空白，看起来像配置丢了。
     */
    @Test
    public void ridParamJsonAllowsCustomInput() {
        assertAllowCustomInput(ModifyObjectRule.class, "ModifyObjectRule.json", "ridParameterName");
        assertAllowCustomInput(DeleteObjectRule.class, "DeleteObjectRule.json", "ridParameterName");
        assertAllowCustomInput(CreateLinkRule.class, "CreateLinkRule.json", "sourceObjectRidParam");
        assertAllowCustomInput(CreateLinkRule.class, "CreateLinkRule.json", "targetObjectRidParam");
        assertAllowCustomInput(DeleteLinkRule.class, "DeleteLinkRule.json", "sourceObjectRidParam");
        assertAllowCustomInput(DeleteLinkRule.class, "DeleteLinkRule.json", "targetObjectRidParam");
    }

    private static void assertSelectable(Class<?> clazz, String fieldName) {
        FormField formField = fieldOf(clazz, fieldName);
        Assert.assertEquals(clazz.getSimpleName() + "." + fieldName
                        + " 必须是 SELECTABLE：INPUTTEXT 时管道下发的候选无处落地，用户回到手打属性名",
                FormFieldType.SELECTABLE, formField.type());
    }

    private static void assertSelectOptionsRegistered(Descriptor<?> descriptor, String fieldName) {
        try {
            List<?> options = descriptor.getSelectOptions(fieldName);
            Assert.assertNotNull(descriptor.getClass().getSimpleName() + "." + fieldName + " 候选不应为 null", options);
        } catch (IllegalStateException e) {
            Assert.assertFalse(descriptor.getClass().getSimpleName() + "." + fieldName + " 未 registerSelectOptions："
                            + e.getMessage(),
                    String.valueOf(e.getMessage()).contains("has not been register"));
        }
    }

    private static void assertAllowCustomInput(Class<?> clazz, String jsonFile, String fieldName) {
        JSONObject json = loadJson(clazz, jsonFile);
        JSONObject fieldProps = json.getJSONObject(fieldName);
        Assert.assertNotNull(jsonFile + " 缺少字段 " + fieldName + " 的描述", fieldProps);
        Assert.assertEquals(jsonFile + "." + fieldName + " 必须打开 allow_custom_input："
                        + "默认值是参数名，不在对象属性候选里，关闭时编辑框显示为空白",
                Boolean.TRUE, fieldProps.getBoolean("allow_custom_input"));
    }

    private static JSONObject loadJson(Class<?> clazz, String jsonFile) {
        try (InputStream is = clazz.getResourceAsStream(jsonFile)) {
            Assert.assertNotNull("找不到插件描述文件：" + jsonFile, is);
            return JSONObject.parseObject(new String(is.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8));
        } catch (java.io.IOException e) {
            throw new AssertionError("读取 " + jsonFile + " 失败", e);
        }
    }

    private static FormField fieldOf(Class<?> clazz, String name) {
        try {
            FormField formField = clazz.getField(name).getAnnotation(FormField.class);
            Assert.assertNotNull(clazz.getSimpleName() + "." + name + " 上没有 @FormField", formField);
            return formField;
        } catch (NoSuchFieldException e) {
            throw new AssertionError(clazz.getSimpleName() + " 上找不到字段 " + name, e);
        }
    }
}