package com.qlangtech.tis.plugin.ontology.impl.infer;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.JSONSerializer;
import com.alibaba.fastjson.serializer.ObjectSerializer;
import com.qlangtech.tis.manage.common.Option;

import java.io.IOException;
import java.lang.reflect.Type;

/**
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/6/9
 * @see InferenceParse
 */
@SuppressWarnings("all")
public class InferenceParseJsonSerializer implements ObjectSerializer {

    @Override
    public void write(JSONSerializer serializer, Object object, Object fieldName, Type fieldType, int features) throws IOException {
        serializer.write(serialize((InferenceParse) object));
    }

    private JSONObject serialize(InferenceParse prop) {
        try {
            JSONObject j = new JSONObject();
            j.put("id", prop.getId());
            j.put(InferenceParse.KEY_NAME, prop.getName());
            j.put(InferenceParse.KEY_REASON, prop.getReason());
            j.put(Option.KEY_END_TYPE, prop.endType().getVal());
            j.put(InferenceParse.KEY_CONFIDENCE, prop.getConfidence().getToken());
            j.put(InferenceParse.KEY_SELECTED, prop.isSelected());
            return j;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
