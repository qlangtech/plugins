package com.qlangtech.tis.plugin.ontology.impl.infer;

import com.qlangtech.tis.extension.OneStepOfMultiSteps;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.Ontology;
import com.qlangtech.tis.plugin.ontology.OntologyLinker;

import java.util.List;
import java.util.Optional;

/**
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/6/8
 */
public class InferOntologyFromLLMStep3Execute extends OneStepOfMultiSteps {

    @FormField(type = FormFieldType.MULTI_SELECTABLE, ordinal = 1, validate = {Validator.require})
    public List<InferenceParse<OntologyLinker>> inferLinkerInstances;

    @TISExtension
    public static final class DftDesc extends BasicDesc {
        @Override
        public Step getStep() {
            return Step.Step5;
        }

        @Override
        public Optional<BasicDesc> nextPluginDesc(OneStepOfMultiSteps current) {
            return Optional.empty();
        }

        @Override
        public String getStepDescription() {
            return Ontology.OntologyEnum.Linker.name() + "推理";
        }
    }
}
