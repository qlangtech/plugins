package com.qlangtech.tis.plugin.ontology;

import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.MultiStepsSupportHost;
import com.qlangtech.tis.extension.MultiStepsSupportHostDescriptor;
import com.qlangtech.tis.extension.OneStepOfMultiSteps;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.IEndTypeGetter;
import com.qlangtech.tis.plugin.IPluginStore;
import com.qlangtech.tis.plugin.IdentityName;
import com.qlangtech.tis.plugin.ontology.impl.infer.InferOntologyFromLLMStep1;
import com.qlangtech.tis.plugin.ontology.impl.infer.InferOntologyFromLLMStep2Execute;
import com.qlangtech.tis.plugin.ontology.impl.infer.InferOntologyFromLLMStep2Prompt;
import com.qlangtech.tis.plugin.ontology.impl.infer.InferOntologyFromLLMStep3Execute;
import com.qlangtech.tis.plugin.ontology.impl.infer.InferOntologyFromLLMStep3Prompt;

import java.util.List;
import java.util.Objects;

/**
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/6/8
 */
public class InferOntologyFromLLMHost extends OntologyDomainManipulate implements IdentityName, MultiStepsSupportHost,
        IPluginStore.ManipuldateProcessor {
    private OneStepOfMultiSteps[] _stepsPlugin;

    @Override
    public void setSteps(OneStepOfMultiSteps[] stepsPlugin) {
        this._stepsPlugin = Objects.requireNonNull(stepsPlugin, "stepsPlugin can not be null");
    }

    @Override
    public OneStepOfMultiSteps[] getMultiStepsSavedItems() {
        return this._stepsPlugin;
    }


    @Override
    public String identityValue() {
        return "";
    }

    @TISExtension
    public static class DftDesc extends Descriptor<InferOntologyFromLLMHost> implements MultiStepsSupportHostDescriptor<InferOntologyFromLLMHost>, IEndTypeGetter, DescriptorUseableShortComment {

        @Override
        public Class<InferOntologyFromLLMHost> getHostClass() {
            return InferOntologyFromLLMHost.class;
        }

        @Override
        public String getDisplayName() {
            return "Infer Ontology From LLM";
        }

        @Override
        public List<OneStepOfMultiSteps.BasicDesc> getStepDescriptionList() {
            return List.of(new InferOntologyFromLLMStep1.DftDesc()
                    , new InferOntologyFromLLMStep2Prompt.DftDesc()
                    , new InferOntologyFromLLMStep2Execute.DftDesc()
                    , new InferOntologyFromLLMStep3Prompt.DftDesc()
                    , new InferOntologyFromLLMStep3Execute.DftDesc());
        }

        @Override
        public void appendExternalProps(JSONObject multiStepsCfg) {

        }

        @Override
        public EndType getEndType() {
            return EndType.Ontology;
        }

        @Override
        public String shortComment() {
            return "推断语义层其他本体实例（如：linkerType等）";
        }
    }
}
