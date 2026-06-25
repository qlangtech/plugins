package com.qlangtech.tis.plugin.ontology.impl.infer.impl;

import com.qlangtech.tis.extension.OneStepOfMultiSteps;
import com.qlangtech.tis.plugin.ontology.Ontology;
import com.qlangtech.tis.plugin.ontology.impl.infer.BaseInferenceParseCreatorFactory;
import com.qlangtech.tis.plugin.ontology.impl.infer.DeserializeOntologyRes;
import com.qlangtech.tis.plugin.ontology.impl.infer.InferOntologyFromLLMStep2Execute;

import java.util.Set;

/**
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/6/11
 */
public class NorLinkTypeInferenceParseCreatorFactory extends BaseInferenceParseCreatorFactory {

    @Override
    protected Set<Ontology.OntologyEnum> getSupportOntologyEnum() {
        return Set.of(Ontology.OntologyEnum.ValueType, Ontology.OntologyEnum.Glossary, Ontology.OntologyEnum.SharedProperty);
    }

    @Override
    protected DeserializeOntologyRes.InferBatch getInferBatch() {
        return DeserializeOntologyRes.InferBatch.NorLinkTypeBatch;
    }

    @Override
    protected Class<? extends OneStepOfMultiSteps> getInferOntologyFromLLMExecuteClass() {
        return InferOntologyFromLLMStep2Execute.class;
    }
}
