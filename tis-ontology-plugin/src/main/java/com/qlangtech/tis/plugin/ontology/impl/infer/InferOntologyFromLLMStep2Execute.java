package com.qlangtech.tis.plugin.ontology.impl.infer;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.extension.OneStepOfMultiSteps;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.Ontology;
import com.qlangtech.tis.util.IPluginContext;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/6/8
 */
@SuppressWarnings("all")
public class InferOntologyFromLLMStep2Execute extends OneStepOfMultiSteps {

    /**
     * 可能有三种类型
     *
     * @see BaseInferenceParseCreatorFactory
     * @see NorLinkTypeInferenceParseCreatorFactory
     */
    @FormField(type = FormFieldType.MULTI_SELECTABLE, ordinal = 1, validate = {Validator.require})
    public List<InferenceParse> inferInstances;

    public static List<InferenceParse> getInferInstances() {
        return Collections.emptyList();
    }

    @Override
    protected void processPreSaved(IPluginContext pluginContext, Context currentCtx, OneStepOfMultiSteps[] preSavedStepPlugins) {
        super.processPreSaved(pluginContext, currentCtx, preSavedStepPlugins);
    }

    @TISExtension
    public static final class DftDesc extends BasicInfterExecuteDesc {
        @Override
        public OneStepOfMultiSteps.Step getStep() {
            return OneStepOfMultiSteps.Step.Step3;
        }

        @Override
        public Optional<BasicDesc> nextPluginDesc(OneStepOfMultiSteps current) {
            return Optional.of(new InferOntologyFromLLMStep3Prompt.DftDesc());
        }

        @Override
        public String getStepDescription() {
            return String.join(",", List.of(Ontology.OntologyEnum.ValueType.name())) + "等资源推理";
        }

        @Override
        protected DeserializeOntologyRes.InferBatch getInferBatch() {
            DeserializeOntologyRes.InferBatch inferBatch = DeserializeOntologyRes.InferBatch.NorLinkTypeBatch;
            return inferBatch;
        }
    }

}
