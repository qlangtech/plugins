package com.qlangtech.tis.plugin.ontology.impl.infer;

import com.qlangtech.tis.extension.OneStepOfMultiSteps;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.ontology.Ontology;

import java.util.Optional;

/**
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/6/8
 */
public class InferOntologyFromLLMStep3Prompt extends OneStepOfMultiSteps {

    @TISExtension
    public static final class DftDesc extends OneStepOfMultiSteps.BasicDesc {
        @Override
        public OneStepOfMultiSteps.Step getStep() {
            return Step.Step4;
        }

        @Override
        public Optional<OneStepOfMultiSteps.BasicDesc> nextPluginDesc(OneStepOfMultiSteps current) {
            return Optional.of(new InferOntologyFromLLMStep3Execute.DftDesc());
        }

        @Override
        public String getStepDescription() {
            return Ontology.OntologyEnum.Linker.name() + "提示词";
        }
    }
}
