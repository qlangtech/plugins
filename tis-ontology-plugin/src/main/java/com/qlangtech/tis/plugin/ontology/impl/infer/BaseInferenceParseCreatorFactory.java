package com.qlangtech.tis.plugin.ontology.impl.infer;

import com.alibaba.citrus.turbine.Context;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.qlangtech.tis.extension.IPropertyType;
import com.qlangtech.tis.extension.OneStepOfMultiSteps;
import com.qlangtech.tis.plugin.ds.CMeta;
import com.qlangtech.tis.plugin.ds.ElementCreatorFactory;
import com.qlangtech.tis.plugin.ds.ViewContent;
import com.qlangtech.tis.plugin.ontology.Ontology;
import com.qlangtech.tis.plugin.ontology.impl.OntologyPluginMeta;
import com.qlangtech.tis.plugin.ontology.impl.glossary.DefaultOntologyGlossary;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.util.DescriptorsJSON;
import org.apache.commons.lang3.StringUtils;

import javax.ws.rs.NotSupportedException;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;

import static com.qlangtech.tis.plugin.ontology.OntologyDomain.NAME_ONTOLOGY_DOMAIN;

/**
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/6/8
 */
public abstract class BaseInferenceParseCreatorFactory implements ElementCreatorFactory<InferenceParse<?>> {
    @Override
    public final CMeta.ParsePostMCols<InferenceParse<?>> parsePostMCols(IPropertyType propertyType
            , IControlMsgHandler msgHandler, Context context, String keyColsMeta, JSONArray targetCols) {
        CMeta.ParsePostMCols<InferenceParse<?>> mCols = new CMeta.ParsePostMCols<>();
        for (Object c : targetCols) {
            if (c instanceof JSONObject col) {
                Integer id = col.getInteger("id");
                Boolean selected = col.getBoolean("selected");
                InferenceParse<DefaultOntologyGlossary> defaultInterence = createDefaultInterence(id);
                // JSON 中没有该字段，视为"默认选中"，返回 true
                defaultInterence.setSelected(selected == null || selected);
                mCols.writerCols.add(defaultInterence);
            } else {
                throw new IllegalStateException("type of c:" + c.getClass().getName() + " must be " + JSONObject.class.getName());
            }
        }
        return mCols;
    }

    @Override
    public final String getTuplesKey() {
        return "_inferResults";
    }

    @Override
    public final void appendExternalJsonProp(IPropertyType propertyType, JSONObject biz) {
        //ElementCreatorFactory.super.appendExternalJsonProp(propertyType, biz);
        OntologyPluginMeta pluginMeta = OntologyPluginMeta.createPluginMeta();
        DeserializeOntologyRes inferOntologyRes = DeserializeOntologyRes.getDomainInferResult(pluginMeta.getDomain());
        List<InferenceParse> inferenceResult = inferOntologyRes.getTargetInferenceParseResult( //
                getSupportOntologyEnum());

        biz.put("inferenceResult", Lists.reverse(inferenceResult));
        biz.put(DescriptorsJSON.KEY_IMPL, getInferOntologyFromLLMExecuteClass().getName());
        biz.put(NAME_ONTOLOGY_DOMAIN, pluginMeta.getDomain());
    }

    protected abstract Set<Ontology.OntologyEnum> getSupportOntologyEnum();

    protected abstract Class<? extends OneStepOfMultiSteps> getInferOntologyFromLLMExecuteClass();

    @Override
    public final ViewContent getViewContentType() {
        return ViewContent.OntologyResInference;
    }

    @Override
    public InferenceParse<?> createDefault(JSONObject targetCol) {
        return createDefaultInterence(0);
    }

    private static InferenceParse<DefaultOntologyGlossary> createDefaultInterence(Integer id) {
        return new InferenceParse<>(id, DeserializeOntologyRes.InferBatch.LinkTypeBatch
                , StringUtils.EMPTY, InferenceParse.InferenceConfidence.Low, new DefaultOntologyGlossary());
    }

    @Override
    public InferenceParse<?> create(JSONObject targetCol, BiConsumer<String, String> errorProcess) {
        throw new NotSupportedException();
    }
}
