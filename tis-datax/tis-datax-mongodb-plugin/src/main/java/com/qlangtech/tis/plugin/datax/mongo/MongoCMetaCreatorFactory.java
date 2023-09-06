package com.qlangtech.tis.plugin.datax.mongo;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.qlangtech.tis.plugin.ds.CMeta;
import com.qlangtech.tis.plugin.ds.DataType;
import org.bson.BsonType;

import java.util.List;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/9/3
 */
public class MongoCMetaCreatorFactory implements CMeta.ElementCreatorFactory {
    private static final String KEY_DOC_FIELD_SPLIT_METAS = "docFieldSplitMetas";

    /**
     * @param targetCol
     * @return
     * @see frontend angular class: RowAssist
     */
    @Override
    public CMeta create(JSONObject targetCol) {
        if (targetCol == null) {
            throw new IllegalArgumentException("param targetCol can not be null");
        }
        MongoCMeta cMeta = new MongoCMeta();
        JSONArray fieldSplitterMetas = null;
        JSONObject fieldSplit = null;
        cMeta.setMongoFieldType(BsonType.valueOf(targetCol.getString("mongoFieldType")));

        if (cMeta.getMongoFieldType() == BsonType.DOCUMENT) {
            List<MongoCMeta.MongoDocSplitCMeta> docFieldSplitMetas = Lists.newArrayList();
            MongoCMeta.MongoDocSplitCMeta splitMeta = null;
            fieldSplitterMetas = targetCol.getJSONArray(KEY_DOC_FIELD_SPLIT_METAS);
            for (Object m : fieldSplitterMetas) {
                fieldSplit = (JSONObject) m;
                splitMeta = new MongoCMeta.MongoDocSplitCMeta();
                splitMeta.setName(fieldSplit.getString("name"));
                splitMeta.setJsonPath(fieldSplit.getString("jsonPath"));
                splitMeta.setType(DataType.parseType(fieldSplit));
                docFieldSplitMetas.add(splitMeta);
            }
            cMeta.setDocFieldSplitMetas(docFieldSplitMetas);
        }

        return cMeta;
    }
}
