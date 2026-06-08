package com.qlangtech.tis.plugin.ontology.impl.infer;

import com.alibaba.citrus.turbine.Context;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.extension.IPropertyType;
import com.qlangtech.tis.plugin.ds.CMeta;
import com.qlangtech.tis.plugin.ds.ElementCreatorFactory;
import com.qlangtech.tis.plugin.ds.ViewContent;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;

import java.util.function.BiConsumer;

/**
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/6/8
 */
public class InferenceParseCreatorFactory implements ElementCreatorFactory<InferenceParse<?>> {
    @Override
    public CMeta.ParsePostMCols<InferenceParse<?>> parsePostMCols(IPropertyType propertyType
            , IControlMsgHandler msgHandler, Context context, String keyColsMeta, JSONArray targetCols) {

        CMeta.ParsePostMCols<InferenceParse<?>> mCols = new CMeta.ParsePostMCols<>();
        return mCols;
    }

    @Override
    public ViewContent getViewContentType() {
        return ViewContent.OntologyResInference;
    }

    @Override
    public InferenceParse<?> createDefault(JSONObject targetCol) {
        return null;
    }

    @Override
    public InferenceParse<?> create(JSONObject targetCol, BiConsumer<String, String> errorProcess) {
        return null;
    }
}
