package com.qlangtech.tis.plugin.ontology;

import com.alibaba.citrus.turbine.Context;
import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.MultiStepsSupportHost;
import com.qlangtech.tis.extension.MultiStepsSupportHostDescriptor;
import com.qlangtech.tis.extension.OneStepOfMultiSteps;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.IEndTypeGetter;
import com.qlangtech.tis.plugin.IPluginStore;
import com.qlangtech.tis.plugin.IdentityName;
import com.qlangtech.tis.plugin.ds.manipulate.ManipulateItemsProcessor;
import com.qlangtech.tis.plugin.ontology.impl.OntologyPluginMeta;
import com.qlangtech.tis.plugin.ontology.impl.infer.DeserializeOntologyRes;
import com.qlangtech.tis.plugin.ontology.impl.infer.InferOntologyFromLLMStep1;
import com.qlangtech.tis.plugin.ontology.impl.infer.InferOntologyFromLLMStep2Execute;
import com.qlangtech.tis.plugin.ontology.impl.infer.InferOntologyFromLLMStep2Prompt;
import com.qlangtech.tis.plugin.ontology.impl.infer.InferOntologyFromLLMStep3Execute;
import com.qlangtech.tis.plugin.ontology.impl.infer.InferOntologyFromLLMStep3Prompt;
import com.qlangtech.tis.util.IPluginContext;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

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
    protected void afterManipuldateProcess(IPluginContext pluginContext
            , Optional<Context> context, ManipulateItemsProcessor itemsProcessor) {
        // super.afterManipuldateProcess(pluginContext, context, itemsProcessor);
        OntologyPluginMeta meta = OntologyPluginMeta.createPluginMeta(itemsProcessor.getPluginMeta());
        DeserializeOntologyRes ontologyRes
                = DeserializeOntologyRes.getDomainInferResult(meta.getDomain());
        // 创建实例，并且在注册器中注销
        int createResCount = ontologyRes.create(pluginContext);
        if (createResCount > 0) {
            pluginContext.addActionMessage(context.orElseThrow(), "已经成功创建" + createResCount + "条Ontology（本体）资源");
        }
    }

    @Override
    public OneStepOfMultiSteps[] getMultiStepsSavedItems() {
        return this._stepsPlugin;
    }


    @Override
    public String identityValue() {
        return "infer_ontology_from_llm";
    }

    @TISExtension
    public static class DftDesc extends BasicDesc implements MultiStepsSupportHostDescriptor<InferOntologyFromLLMHost>, IEndTypeGetter, DescriptorUseableShortComment {

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
