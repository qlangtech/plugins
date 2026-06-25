package com.qlangtech.tis.plugin.ontology.impl.glossary;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.aiagent.llm.ITISJsonSchema;
import com.qlangtech.tis.aiagent.llm.TISJsonSchema;
import com.qlangtech.tis.extension.MultiStepsSupportHost;
import com.qlangtech.tis.plugin.ontology.impl.valuetype.DefaultOntologyValueType;
import com.qlangtech.tis.trigger.util.JsonUtil;
import com.qlangtech.tis.util.DescriptorsJSONForAIPrompt;
import com.qlangtech.tis.util.DescriptorsMeta;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static com.qlangtech.tis.aiagent.llm.TISJsonSchema.SCHEMA_ONE_OF;
import static com.qlangtech.tis.aiagent.llm.TISJsonSchema.SCHEMA_PROPERTIES;
import static com.qlangtech.tis.aiagent.llm.TISJsonSchema.SCHEMA_VALUE_CONST;
import static com.qlangtech.tis.util.AttrValMap.PLUGIN_EXTENSION_IMPL;
import static com.qlangtech.tis.util.AttrValMap.PLUGIN_EXTENSION_VALS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/6/22
 */
public class DefaultOntologyGlossaryTest {
    /**
     * 回归对照：非 MultiStepsSupportHost 的 host（OntologyGlossary）schema 不受核心库变更影响，
     * 仍然保持 vals 平铺业务字段、不出现 multiStepsSavedItems。
     */
    @Test
    public void testNonMultiStepsSchemaUnchanged_OntologyGlossary() {
        DescriptorsJSONForAIPrompt descriptorsJSON =
                new DescriptorsJSONForAIPrompt<>(Collections.singletonList(new DefaultOntologyGlossary.DefaultDesc()), true);
        DescriptorsJSONForAIPrompt.AISchemaDescriptorsMeta meta =
                (DescriptorsJSONForAIPrompt.AISchemaDescriptorsMeta) descriptorsJSON.getDescriptorsJSON();

        TISJsonSchema schema = meta.descSchemaRegister.values().iterator().next().getKey();
        System.out.println("=== OntologyGlossary Schema (regression) ===");
        System.out.println(JsonUtil.toString(schema.root(), true));

        JSONObject hostProps = schema.schema().getJSONObject(SCHEMA_PROPERTIES);
        JSONObject valsInner = hostProps.getJSONObject(PLUGIN_EXTENSION_VALS).getJSONObject(SCHEMA_PROPERTIES);

        assertFalse("Glossary should NOT have multiStepsSavedItems",
                valsInner.containsKey(MultiStepsSupportHost.KEY_MULTI_STEPS_SAVED_ITEMS));
        assertTrue("Glossary vals.term", valsInner.containsKey("term"));
        assertTrue("Glossary vals.synonyms", valsInner.containsKey("synonyms"));
        assertTrue("Glossary vals.description", valsInner.containsKey("description"));
        assertTrue("Glossary vals.target", valsInner.containsKey("target"));
    }

    /**
     * 验证 MultiStepsSupportHost 类型 host 的 schema 自动展开为
     * <code>vals.multiStepsSavedItems[oneOf{stepImpl, stepVals}]</code>。
     * 与 {@link com.qlangtech.tis.extension.OneStepOfMultiSteps#parseStepsPlugin} 期望的反序列化格式天然对齐。
     */
    @Test
    public void testMultiStepsSchemaForOntologyValueType() {
        DefaultOntologyValueType.DefaultDesc hostDesc = new DefaultOntologyValueType.DefaultDesc();
        DescriptorsJSONForAIPrompt descriptorsJSON =
                new DescriptorsJSONForAIPrompt<>(Collections.singletonList(hostDesc), true);
        DescriptorsMeta meta = descriptorsJSON.getDescriptorsJSON();

        ITISJsonSchema hostSchema = meta.getPluginJsonSchema().values().iterator().next();
        System.out.println("=== OntologyValueType Schema ===");
        System.out.println(JsonUtil.toString(hostSchema.root(), true));

        JSONObject hostProps = hostSchema.schema().getJSONObject(SCHEMA_PROPERTIES);
        assertNotNull("host properties must exist", hostProps);

        // impl const = host descriptor id
        JSONObject implProp = hostProps.getJSONObject(PLUGIN_EXTENSION_IMPL);
        assertNotNull(implProp);
        assertEquals("host impl const must equal descriptor id",
                hostDesc.getId(), implProp.getString(SCHEMA_VALUE_CONST));

        // vals.multiStepsSavedItems
        JSONObject valsProp = hostProps.getJSONObject(PLUGIN_EXTENSION_VALS);
        assertNotNull(valsProp);
        JSONObject valsInnerProps = valsProp.getJSONObject(SCHEMA_PROPERTIES);
        assertNotNull(valsInnerProps);
        JSONObject multiSteps = valsInnerProps.getJSONObject(MultiStepsSupportHost.KEY_MULTI_STEPS_SAVED_ITEMS);
        assertNotNull("multiStepsSavedItems must exist", multiSteps);
        assertEquals("array", multiSteps.getString("type"));

        JSONObject items = multiSteps.getJSONObject("items");
        assertNotNull(items);
        JSONArray oneOf = items.getJSONArray(SCHEMA_ONE_OF);
        assertNotNull("items.oneOf must exist", oneOf);
        assertEquals("Should have 2 step variants (Metadata + Constraints)", 2, oneOf.size());

        Set<String> stepImplConsts = new HashSet<>();
        for (int i = 0; i < oneOf.size(); i++) {
            JSONObject stepSchema = oneOf.getJSONObject(i);
            JSONObject stepProps = stepSchema.getJSONObject(SCHEMA_PROPERTIES);
            assertNotNull("step" + i + " must have properties", stepProps);
            JSONObject stepImpl = stepProps.getJSONObject(PLUGIN_EXTENSION_IMPL);
            assertNotNull("step" + i + " must have impl", stepImpl);
            stepImplConsts.add(stepImpl.getString(SCHEMA_VALUE_CONST));
            assertTrue("step" + i + " must have vals", stepProps.containsKey(PLUGIN_EXTENSION_VALS));
        }
        // 两个 step 的 impl 必须不同（独立的 vals 容器，不会互相覆盖）
        assertEquals("Two step variants must have distinct impl ids", 2, stepImplConsts.size());

        // 验证 step1 (MetadataOfValueType) 的 vals 含 name / description / type
        // 而且 type 字段已通过 MetadataOfValueType.json 的 enum 表达式注入了候选值
        JSONObject metadataStep = findStepByImplName(oneOf, "MetadataOfValueType");
        assertNotNull("MetadataOfValueType step not found", metadataStep);
        JSONObject metadataVals = metadataStep.getJSONObject(SCHEMA_PROPERTIES)
                .getJSONObject(PLUGIN_EXTENSION_VALS).getJSONObject(SCHEMA_PROPERTIES);
        assertTrue("vals.name", metadataVals.containsKey("name"));
        assertTrue("vals.description", metadataVals.containsKey("description"));
        assertTrue("vals.type", metadataVals.containsKey("type"));
        JSONObject typeProp = metadataVals.getJSONObject("type");
        assertTrue("type field should carry enum candidates from MetadataOfValueType.json",
                typeProp.containsKey("enum"));

        // 验证 step2 (ConstraintsOfValueType) 的 vals.constraint 是 oneOf 多态
        JSONObject constraintsStep = findStepByImplName(oneOf, "ConstraintsOfValueType");
        assertNotNull("ConstraintsOfValueType step not found", constraintsStep);
        JSONObject constraintsVals = constraintsStep.getJSONObject(SCHEMA_PROPERTIES)
                .getJSONObject(PLUGIN_EXTENSION_VALS).getJSONObject(SCHEMA_PROPERTIES);
        JSONObject constraintField = constraintsVals.getJSONObject("constraint");
        assertNotNull("vals.constraint must exist", constraintField);
        JSONArray constraintOneOf = constraintField.getJSONArray(SCHEMA_ONE_OF);
        assertNotNull("constraint must be oneOf", constraintOneOf);
        assertTrue("constraint oneOf must include Enum / Range / Regex variants, count=" + constraintOneOf.size(),
                constraintOneOf.size() >= 7);
    }

    private static JSONObject findStepByImplName(JSONArray oneOf, String simpleName) {
        for (int i = 0; i < oneOf.size(); i++) {
            JSONObject stepSchema = oneOf.getJSONObject(i);
            String impl = stepSchema.getJSONObject(SCHEMA_PROPERTIES)
                    .getJSONObject(PLUGIN_EXTENSION_IMPL).getString(SCHEMA_VALUE_CONST);
            if (impl != null && impl.endsWith("." + simpleName)) {
                return stepSchema;
            }
        }
        return null;
    }
}