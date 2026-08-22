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

import java.util.List;
import java.util.Optional;

import static com.qlangtech.tis.plugin.ontology.impl.infer.InferOntologyFromLLMStep1.getOntologyPluginMeta;

/**
 * 确认valueType的提示词
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/6/8
 */
public class InferOntologyFromLLMStep2Prompt extends OneStepOfMultiSteps {

    @FormField(type = FormFieldType.TEXTAREA, ordinal = 1, validate = {Validator.require})
    public String glossaryPrompt;

    @FormField(type = FormFieldType.ENUM, ordinal = 2, validate = {Validator.require})
    public Boolean enableValueTypeInfer;

    @FormField(type = FormFieldType.TEXTAREA, ordinal = 3, validate = {Validator.require})
    public String valueTypePrompt;

    @FormField(type = FormFieldType.ENUM, ordinal = 4, validate = {Validator.require})
    public Boolean enableSharedPropertyInfer;

    @FormField(type = FormFieldType.TEXTAREA, ordinal = 5, validate = {Validator.require})
    public String sharedPropertyPrompt;

    @Override
    protected void processPreSaved(IPluginContext pluginContext, Context ctx, OneStepOfMultiSteps[] preSavedStepPlugins) {
        InferOntologyFromLLMStep1 step1 = (InferOntologyFromLLMStep1) preSavedStepPlugins[Step.Step1.getStepIndex()];
        OntologyPluginMeta ometa = getOntologyPluginMeta(pluginContext, Optional.of(ctx));

        DeserializeOntologyRes.getOntologyResInfer(ometa.getDomain(), pluginContext, ctx, this, step1);


    }

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
