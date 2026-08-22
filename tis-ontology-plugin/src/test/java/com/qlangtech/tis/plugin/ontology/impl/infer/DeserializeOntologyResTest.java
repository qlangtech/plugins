package com.qlangtech.tis.plugin.ontology.impl.infer;

import org.apache.commons.lang3.tuple.Triple;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

/**
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/6/9
 */
public class DeserializeOntologyResTest {

    @Test
    public void testBuildSystemPrompt() {
        DeserializeOntologyRes res = new DeserializeOntologyRes("order2", Collections.emptyList(), null);

        String systemPrompt = res.buildSystemPrompt(
                Triple.of(OntologyResourceInferenceConfig.sharedPropertyConfig, OntologyResourceInferenceConfig.sharedPropertyConfig.getPrompt(), true)
                , Triple.of(OntologyResourceInferenceConfig.valueType, OntologyResourceInferenceConfig.valueType.getPrompt(), true)
                , Triple.of(OntologyResourceInferenceConfig.glossary, OntologyResourceInferenceConfig.glossary.getPrompt(), true));
        Assert.assertNotNull(systemPrompt);
        System.out.println(systemPrompt);
    }
}