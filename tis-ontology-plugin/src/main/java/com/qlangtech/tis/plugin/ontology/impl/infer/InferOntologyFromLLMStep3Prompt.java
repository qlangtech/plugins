package com.qlangtech.tis.plugin.ontology.impl.infer;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.extension.OneStepOfMultiSteps;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.Ontology;
import com.qlangtech.tis.plugin.ontology.impl.OntologyPluginMeta;
import com.qlangtech.tis.util.IPluginContext;

import java.util.Optional;

import static com.qlangtech.tis.plugin.ontology.impl.infer.InferOntologyFromLLMStep1.getOntologyPluginMeta;

/**
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/6/8
 */
public class InferOntologyFromLLMStep3Prompt extends OneStepOfMultiSteps {

    @FormField(type = FormFieldType.TEXTAREA, ordinal = 2, validate = {Validator.require})
    public String linkTypePrompt;


    @Override
    protected void processPreSaved(IPluginContext pluginContext, Context currentCtx, OneStepOfMultiSteps[] preSavedStepPlugins) {

        InferOntologyFromLLMStep1 step1 = (InferOntologyFromLLMStep1) preSavedStepPlugins[Step.Step1.getStepIndex()];
        OntologyPluginMeta ometa = getOntologyPluginMeta(pluginContext, Optional.of(currentCtx));

        DeserializeOntologyRes.getOntologyResInfer(ometa.getDomain(), pluginContext, currentCtx, this, step1);
    }

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
