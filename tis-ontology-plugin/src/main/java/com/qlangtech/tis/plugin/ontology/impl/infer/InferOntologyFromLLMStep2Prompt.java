package com.qlangtech.tis.plugin.ontology.impl.infer;

import com.qlangtech.tis.extension.OneStepOfMultiSteps;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.Ontology;

import java.util.List;
import java.util.Optional;

/**
 * 确认valueType的提示词
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/6/8
 */
public class InferOntologyFromLLMStep2Prompt extends OneStepOfMultiSteps {

    @FormField(type = FormFieldType.TEXTAREA, ordinal = 1, validate = {Validator.require})
    public String valueTypePrompt;

    @FormField(type = FormFieldType.TEXTAREA, ordinal = 2, validate = {Validator.require})
    public String glossaryPrompt;

    @FormField(type = FormFieldType.TEXTAREA, ordinal = 3, validate = {Validator.require})
    public String sharedPropertyPrompt;


    @TISExtension
    public static final class DftDesc extends OneStepOfMultiSteps.BasicDesc {
        @Override
        public OneStepOfMultiSteps.Step getStep() {
            return Step.Step2;
        }

        @Override
        public Optional<OneStepOfMultiSteps.BasicDesc> nextPluginDesc(OneStepOfMultiSteps current) {
            return Optional.of(new InferOntologyFromLLMStep2Execute.DftDesc());
        }

        @Override
        public String getStepDescription() {
            return String.join(",", List.of(Ontology.OntologyEnum.ValueType.name())) + "等提示词设置";
        }
    }
}
