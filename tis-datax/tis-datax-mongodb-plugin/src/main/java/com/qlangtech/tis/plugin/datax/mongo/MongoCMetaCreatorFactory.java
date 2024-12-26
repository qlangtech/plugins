package com.qlangtech.tis.plugin.datax.mongo;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.qlangtech.tis.plugin.datax.SelectedTab;
import com.qlangtech.tis.plugin.ds.CMeta;
import com.qlangtech.tis.plugin.ds.IdlistElementCreatorFactory;
import org.bson.BsonType;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import static com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler.joinField;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/9/3
 */
public class MongoCMetaCreatorFactory extends IdlistElementCreatorFactory {
    public static final String KEY_DOC_FIELD_SPLIT_METAS = "docFieldSplitMetas";
    public static final String KEY_JSON_PATH = "jsonPath";

//    @Override
//    public ParsePostMCols<CMeta> parsePostMCols(IPropertyType propertyType, IControlMsgHandler msgHandler, Context context, String keyColsMeta, JSONArray targetCols) {
//        if (targetCols == null) {
//            throw new IllegalArgumentException("param targetCols can not be null");
//        }
//        CMeta.ParsePostMCols postMCols = new CMeta.ParsePostMCols();
//        CMeta colMeta = null;
//
//
//        String targetColName = null;
//        DataType dataType = null;
//
//        Map<String, Integer> existCols = Maps.newHashMap();
//        Integer previousColIndex = null;
//        boolean pk;
//        for (int i = 0; i < targetCols.size(); i++) {
//            JSONObject targetCol = targetCols.getJSONObject(i);
//            int index = targetCol.getInteger("index") - 1;
//            pk = targetCol.getBooleanValue("pk");
//            targetColName = targetCol.getString("name");
//            if (StringUtils.isNotBlank(targetColName) && (previousColIndex = existCols.put(targetColName, index)) != null) {
//                msgHandler.addFieldError(context, keyColsMeta + "[" + previousColIndex + "]", "内容不能与第" + index + "行重复");
//                msgHandler.addFieldError(context, keyColsMeta + "[" + index + "]", "内容不能与第" + previousColIndex + "行重复");
//                // return false;
//                postMCols.validateFaild = true;
//                return postMCols;
//            }
//            if (!Validator.require.validate(msgHandler, context, keyColsMeta + "[" + index + "]", targetColName)) {
//                postMCols.validateFaild = true;
//            } else if (!Validator.db_col_name.validate(msgHandler, context, keyColsMeta + "[" + index + "]",
//                    targetColName)) {
//                postMCols.validateFaild = true;
//            }
//
//
//            colMeta = this.create(targetCol, (propKey, errMsg) -> {
//                msgHandler.addFieldError(context
//                        , IFieldErrorHandler.joinField(SelectedTab.KEY_FIELD_COLS, Collections.singletonList(index), propKey)
//                        , errMsg);
//                postMCols.validateFaild = true;
//            });
//
//            if (pk) {
//                postMCols.pkHasSelected = true;
//            }
//
//            dataType = CMeta.parseType(targetCol, (propKey, errMsg) -> {
//                msgHandler.addFieldError(context
//                        , IFieldErrorHandler.joinField(SelectedTab.KEY_FIELD_COLS, Collections.singletonList(index), propKey)
//                        , errMsg);
//                postMCols.validateFaild = true;
//            });
//
//            if (dataType != null) {
//                colMeta.setType(dataType);
//                postMCols.writerCols.add(colMeta);
//            }
//        }
//
//        return postMCols;
//
//
//
//    }

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
     * @see //frontend angular class: RowAssist
     */
    @Override
    public CMeta create(JSONObject targetCol, BiConsumer<String, String> errorProcess) {
        MongoCMeta cMeta = (MongoCMeta) super.create(targetCol, errorProcess);

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
