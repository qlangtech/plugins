package com.qlangtech.tis.plugin.datax.mongo;

import com.qlangtech.tis.plugin.ds.CMeta;
import com.qlangtech.tis.plugin.ds.ColumnMetaData;
import org.apache.commons.lang.StringUtils;
import org.bson.BsonType;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/9/2
 */
public class MongoCMeta extends CMeta {

    public static final String KEY_MONOG_NEST_PROP_SEPERATOR = ".";
    private BsonType mongoFieldType;


    private List<MongoDocSplitCMeta> docFieldSplitMetas;


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
        return this.docFieldSplitMetas;
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

    public static class MongoDocSplitCMeta extends MongoCMeta {
        private String jsonPath;

        public String getJsonPath() {
            return jsonPath;
        }

        public void setJsonPath(String jsonPath) {
            this.jsonPath = jsonPath;
        }
    }
}
