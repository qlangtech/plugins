package com.qlangtech.tis.plugin.ontology.impl.infer.impl;

import com.qlangtech.tis.extension.OneStepOfMultiSteps;
import com.qlangtech.tis.plugin.ontology.Ontology;
import com.qlangtech.tis.plugin.ontology.impl.infer.BaseInferenceParseCreatorFactory;
import com.qlangtech.tis.plugin.ontology.impl.infer.InferOntologyFromLLMStep3Execute;

import java.util.Set;

/**
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/6/11
 */
public class LinkTypeInferenceParseCreatorFactory extends BaseInferenceParseCreatorFactory {
    @Override
    protected Set<Ontology.OntologyEnum> getSupportOntologyEnum() {
        return Set.of(Ontology.OntologyEnum.Linker);
    }

    @Override
    protected Class<? extends OneStepOfMultiSteps> getInferOntologyFromLLMExecuteClass() {
        return InferOntologyFromLLMStep3Execute.class;
    }
}
