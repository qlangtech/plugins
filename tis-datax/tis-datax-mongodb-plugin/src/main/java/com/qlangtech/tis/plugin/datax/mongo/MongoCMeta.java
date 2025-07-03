package com.qlangtech.tis.plugin.datax.mongo;

import com.alibaba.fastjson.annotation.JSONField;
import com.google.common.collect.Lists;
import com.qlangtech.tis.plugin.ds.CMeta;
import com.qlangtech.tis.plugin.ds.CMeta.INestCMetaGetter;
import com.qlangtech.tis.plugin.ds.ColumnMetaData;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.bson.BsonDocument;
import org.bson.BsonType;
import org.bson.BsonValue;

import java.time.ZoneId;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/9/2
 */
public class MongoCMeta extends CMeta implements INestCMetaGetter {

    public static final String KEY_MONOG_NEST_PROP_SEPERATOR = ".";
    private BsonType mongoFieldType;

    public static <RESULT_TYPE> List<Pair<MongoCMeta, Function<BsonDocument, RESULT_TYPE>>>
    getMongoPresentCols(List<MongoCMeta> mongoCols, boolean dataXColumType, ZoneId zone) {
        // List<MongoCMeta> mongoCols = table.getCols().stream().map((c) -> (MongoCMeta) c).collect(Collectors.toList());
        if (CollectionUtils.isEmpty(mongoCols)) {
            throw new IllegalArgumentException("mongoCols can not be null");
        }
        List<Pair<MongoCMeta, Function<BsonDocument, RESULT_TYPE>>> presentCols = Lists.newArrayList();

        //  List<MongoCMeta.MongoDocSplitCMeta> splitFieldMetas = null;
        for (CMeta col : mongoCols) {

            MongoCMeta mongoCol = (MongoCMeta) col;
//            if (mongoCol.isMongoDocType()) {
//                splitFieldMetas = mongoCol.getDocFieldSplitMetas();
//                for (MongoCMeta.MongoDocSplitCMeta scol : splitFieldMetas) {
//                    presentCols.add(Pair.of(scol, MongoColValGetter.create(scol, dataXColumType, zone)));
//                }
//            }

            if (col.isDisable()) {
                continue;
            }


//            presentCols.add(Pair.of(mongoCol, (doc) -> {
//                BsonValue val = doc.get(mongoCol.getName());
//                //  Object val = doc.get(mongoCol.getName(), Object.class);
//                return MongoDataXColUtils.createCol(mongoCol, val);
//            }));

            presentCols.add(Pair.of(mongoCol, MongoColValGetter.create(mongoCol, dataXColumType, zone)));
        }
        if (CollectionUtils.isEmpty(presentCols)) {
            throw new IllegalStateException("presetnCols can not be empty");
        }
        return Collections.unmodifiableList(presentCols);
    }


    public BsonValue getBsonVal(BsonDocument document) {
        return document.get(this.getName());
    }

    private List<MongoDocSplitCMeta> docFieldSplitMetas;

    public void setDocFieldSplitMetas(List<MongoDocSplitCMeta> docFieldSplitMetas) {
        this.docFieldSplitMetas = docFieldSplitMetas;
    }

    public void setFlatMapDocumentTypes(List<ColumnMetaData> flatMapDocumentTypes) {
        docFieldSplitMetas = flatMapDocumentTypes.stream().map((c) -> {
            MongoColumnMetaData cc = (MongoColumnMetaData) c;
            MongoDocSplitCMeta cmeta = new MongoDocSplitCMeta();
            cmeta.setJsonPath(c.getName());
            cmeta.setName(StringUtils.replace(c.getName(), KEY_MONOG_NEST_PROP_SEPERATOR, "_"));
            cmeta.setType(c.getType());
            cmeta.setMongoFieldType(cc.getMongoFieldType());
            return cmeta;
        }).collect(Collectors.toList());
    }

    public List<MongoDocSplitCMeta> getDocFieldSplitMetas() {
        return CollectionUtils.isEmpty(this.docFieldSplitMetas) ? Collections.emptyList() : this.docFieldSplitMetas;
    }

    public BsonType getMongoFieldType() {
        return mongoFieldType;
    }

    public void setMongoFieldType(BsonType mongoFieldType) {
        this.mongoFieldType = mongoFieldType;
    }

    public boolean isMongoDocType() {
        return this.mongoFieldType == BsonType.DOCUMENT;
    }

    @Override
    public List<MongoDocSplitCMeta> nestCols() {
        return isMongoDocType() ? getDocFieldSplitMetas() : Collections.emptyList();
    }

    public static class MongoDocSplitCMeta extends MongoCMeta {
        public static final Pattern PATTERN_JSON_PATH //
                = Pattern.compile("([a-zA-Z0-9](_?[a-zA-Z0-9])*_?)(\\.[a-zA-Z0-9](_?[a-zA-Z0-9])*_?)*");
        private String jsonPath;

        public String getJsonPath() {
            return jsonPath;
        }

        private List<String> embeddedKeys;

        @JSONField(serialize = false)
        public List<String> getEmbeddedKeys() {
            if (this.embeddedKeys == null) {
                this.embeddedKeys = Lists.newArrayList(StringUtils.split(getJsonPath(), KEY_MONOG_NEST_PROP_SEPERATOR));
                if (CollectionUtils.isEmpty(this.embeddedKeys)) {
                    throw new IllegalStateException("embeddedKeys can not be null");
                }
            }
            return this.embeddedKeys;
        }

        @Override
        public BsonValue getBsonVal(BsonDocument document) {
            return getEmbeddedValue(this.getEmbeddedKeys(), document);
        }

        private static BsonValue getEmbeddedValue(List<String> keys, BsonDocument doc) {
            BsonValue value = null;
            String key = null;
            Iterator<String> keyIterator = keys.iterator();

            while (keyIterator.hasNext()) {
                key = keyIterator.next();
                value = doc.get(key);
                if (value == null || value.isNull()) {
                    return null;
                }
                if (value.isDocument()) {
                    doc = value.asDocument();
                } else {
                    if (keyIterator.hasNext()) {
                        throw new IllegalStateException(String.format("At key %s, the value is not a BsonDocument (%s)", key, value.getClass().getName()));
                    }
                    return value;
                }
            }

            return doc;
        }

        @Override
        public void setDocFieldSplitMetas(List<MongoDocSplitCMeta> docFieldSplitMetas) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isMongoDocType() {
            return false;
        }

        @Override
        public List<MongoDocSplitCMeta> getDocFieldSplitMetas() {
            return Collections.emptyList();
        }

        public void setJsonPath(String jsonPath) {
            this.jsonPath = jsonPath;
        }
    }
}
