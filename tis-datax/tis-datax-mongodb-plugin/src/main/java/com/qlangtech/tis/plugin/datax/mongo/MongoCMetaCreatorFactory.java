package com.qlangtech.tis.plugin.datax.mongo;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.qlangtech.tis.plugin.datax.SelectedTab;
import com.qlangtech.tis.plugin.ds.CMeta;
import org.bson.BsonType;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import static com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler.joinField;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/9/3
 */
public class MongoCMetaCreatorFactory implements CMeta.ElementCreatorFactory {
    public static final String KEY_DOC_FIELD_SPLIT_METAS = "docFieldSplitMetas";
    public static final String KEY_JSON_PATH = "jsonPath";

    /**
     * example:   cols[0].docFieldSplitMetas[0].jsonpath 等等
     *
     * @param colIndex
     * @param docSplitFieldIndex
     * @param fieldKey
     * @return
     */
    public static String splitMetasKey(int colIndex, int docSplitFieldIndex, String fieldKey) {
        return joinField(SelectedTab.KEY_FIELD_COLS, Lists.newArrayList(colIndex), splitMetasKey(docSplitFieldIndex, fieldKey));
    }

    public static String splitMetasKey(int docSplitFieldIndex, String fieldKey) {
        return joinField(
                KEY_DOC_FIELD_SPLIT_METAS, Lists.newArrayList(docSplitFieldIndex),
                fieldKey);
    }

    @Override
    public CMeta createDefault() {
        return new MongoCMeta();
    }

    /**
     * @param targetCol
     * @return
     * @see frontend angular class: RowAssist
     */
    @Override
    public CMeta create(JSONObject targetCol, BiConsumer<String, String> errorProcess) {
        if (targetCol == null) {
            throw new IllegalArgumentException("param targetCol can not be null");
        }
        MongoCMeta cMeta = (MongoCMeta) createDefault();
        JSONArray fieldSplitterMetas = null;
        JSONObject fieldSplit = null;
        cMeta.setMongoFieldType(BsonType.valueOf(targetCol.getString("mongoFieldType")));

        if (cMeta.getMongoFieldType() == BsonType.DOCUMENT) {
            List<MongoCMeta.MongoDocSplitCMeta> docFieldSplitMetas = Lists.newArrayList();
            MongoCMeta.MongoDocSplitCMeta splitMeta = null;
            fieldSplitterMetas = targetCol.getJSONArray(KEY_DOC_FIELD_SPLIT_METAS);
            AtomicInteger docSplitFieldIndex = new AtomicInteger();
            for (Object m : fieldSplitterMetas) {
                fieldSplit = (JSONObject) m;
                splitMeta = new MongoCMeta.MongoDocSplitCMeta();
                splitMeta.setName(fieldSplit.getString("name"));
                splitMeta.setJsonPath(fieldSplit.getString(KEY_JSON_PATH));
                splitMeta.setType(CMeta.parseType(fieldSplit, (key, errMsg) -> {
                    errorProcess.accept(splitMetasKey(docSplitFieldIndex.get(), key), errMsg);
                }));
                docFieldSplitMetas.add(splitMeta);
                docSplitFieldIndex.incrementAndGet();
            }
            cMeta.setDocFieldSplitMetas(docFieldSplitMetas);
        }

        return cMeta;
    }
}
