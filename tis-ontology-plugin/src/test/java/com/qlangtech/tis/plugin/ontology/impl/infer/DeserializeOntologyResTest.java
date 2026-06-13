package com.qlangtech.tis.plugin.ontology.impl.infer;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/6/9
 */
public class DeserializeOntologyResTest {

    @Test
    public void testBuildSystemPrompt() {
        DeserializeOntologyRes res = new DeserializeOntologyRes("order2", null);

        String systemPrompt = res.buildSystemPrompt(
                Pair.of(OntologyResourceInferenceConfig.sharedPropertyConfig, OntologyResourceInferenceConfig.sharedPropertyConfig.getPrompt())
                , Pair.of(OntologyResourceInferenceConfig.valueType, OntologyResourceInferenceConfig.valueType.getPrompt())
                , Pair.of(OntologyResourceInferenceConfig.glossary, OntologyResourceInferenceConfig.glossary.getPrompt()));
        Assert.assertNotNull(systemPrompt);
        System.out.println(systemPrompt);
    }
}