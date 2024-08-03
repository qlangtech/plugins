package com.qlangtech.tis.plugin.datax.mongo;

import com.alibaba.fastjson.annotation.JSONField;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.mongodb.MongoClient;
import com.qlangtech.tis.plugin.ds.*;
import org.apache.commons.collections.ListUtils;
import org.bson.BsonDocument;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.Document;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/9/1
 */
public class MongoColumnMetaData extends ColumnMetaData {
    private final BsonType mongoFieldType;

    /**
     * 检测该列是在全部检测的列中含有值的数量
     */
    private int containValCount;
    private int maxStrLength;

    /**
     * 存放docuemt类型字段的flatmap 之后的key-> val
     */
    private final Map<String, MongoColumnMetaData> docTypeFieldEnum = Maps.newHashMap();

    public MongoColumnMetaData(int index, String key, BsonType mongoFieldType) {
        this(index, key, mongoFieldType, 0);
    }

    public MongoColumnMetaData(int index, String key, BsonType mongoFieldType, int containValCount) {
        this(index, key, mongoFieldType, containValCount, false);
    }

    public MongoColumnMetaData(int index, String key, BsonType mongoFieldType, int containValCount, boolean pk) {
        this(index, key, mapType(mongoFieldType), mongoFieldType, containValCount, pk);
    }

    public MongoColumnMetaData(int index, String key, DataType dataType, BsonType mongoFieldType, int containValCount
            , boolean pk) {
        super(index, key, dataType, pk);
        this.mongoFieldType = mongoFieldType;
        this.containValCount = containValCount;
    }


    public static void parseMongoDocTypes(
            Map<String, MongoColumnMetaData> colsSchema, Document doc) {
        BsonDocument bdoc = doc.toBsonDocument(BsonDocument.class, MongoClient.getDefaultCodecRegistry());
        parseMongoDocTypes(false, Collections.emptyList(), colsSchema, bdoc);
    }

    /**
     * @param parseChildDoc 是否遍历所有子doc？
     * @param parentKeys
     * @param colsSchema
     * @param bdoc
     */
    public static void parseMongoDocTypes(boolean parseChildDoc, List<String> parentKeys //
            , Map<String, MongoColumnMetaData> colsSchema, BsonDocument bdoc) {
        int index = 0;
        BsonValue val;
        String key;
        MongoColumnMetaData colMeta;
        List<String> keys = null;

        for (Map.Entry<String, BsonValue> entry : bdoc.entrySet()) {
            val = entry.getValue();
            keys = ListUtils.union(parentKeys, Collections.singletonList(entry.getKey()));
            key = String.join(MongoCMeta.KEY_MONOG_NEST_PROP_SEPERATOR, keys);
            colMeta = colsSchema.get(key);

            try {

                if (colMeta == null) {
                    colMeta = new MongoColumnMetaData(index, key, val.getBsonType(), 0,
                            (val.getBsonType() == BsonType.OBJECT_ID));
                    colsSchema.put(key, colMeta);
                } else {
                    if (colMeta.getMongoFieldType() == BsonType.NULL && !val.isNull()) {
                        colMeta = new MongoColumnMetaData(index, key, val.getBsonType());
                        colsSchema.put(key, colMeta);
                    } else if (colMeta.getMongoFieldType() != BsonType.STRING //
                            && !val.isNull() && colMeta.getMongoFieldType() != val.getBsonType()) {
                        //TODO： 前后两次类型不同
                        // 则直接将类型改成String类型
                        colMeta = new MongoColumnMetaData(index, key, BsonType.STRING);
                        colsSchema.put(key, colMeta);
                    }
                }
                if (!val.isNull()) {

                    if (colMeta.getMongoFieldType() == BsonType.DOCUMENT && val.isDocument()) {
                        parseMongoDocTypes(true, keys, parseChildDoc ? colsSchema : colMeta.docTypeFieldEnum, val.asDocument());
                    }


                    if (colMeta.getMongoFieldType() == BsonType.STRING) {
                        colMeta.setMaxStrLength(val.asString().getValue().length());
                    }

                    colMeta.incrContainValCount();
                }
            } catch (Exception e) {
                throw new RuntimeException("key:" + key + ",val:" + val, e);
            } finally {
                index++;
            }

        }
    }

    /**
     * 重新整理列表
     *
     * @param colsSchema
     * @return
     */
    public static List<ColumnMetaData> reorder(Map<String, MongoColumnMetaData> colsSchema) {
        List<ColumnMetaData> result = Lists.newArrayList(colsSchema.values());
        MongoColumnMetaData col = null;
        for (int i = 0; i < result.size(); i++) {
            col = (MongoColumnMetaData) result.get(i);

            if (col.getType() == null) {
                //  int index, String key, DataType type, boolean pk, boolean nullable
                result.set(i, new MongoColumnMetaData(col.getIndex(), col.getKey(), BsonType.STRING));
            } else if (col.getMongoFieldType() == BsonType.STRING) {
                if (col.getMaxStrLength() > col.getType().getColumnSize()) {
                    // 调整String cols size
                    if (col.getMaxStrLength() > DataTypeMeta.getDataTypeMeta(JDBCTypes.VARCHAR).getColsSizeRange().getMax()) {
                        // 超过了varchar colsSize的上限了直接设置为TEXT（LONGVARCHAR）类型
                        result.set(i, new MongoColumnMetaData(col.getIndex(), col.getKey(),
                                DataType.getType(JDBCTypes.LONGVARCHAR), BsonType.STRING, col.getContainValCount(),
                                false));
                    } else {
                        result.set(i, new MongoColumnMetaData(col.getIndex(), col.getKey(),
                                DataType.create(JDBCTypes.VARCHAR.getType(), JDBCTypes.VARCHAR.getLiteria(),
                                        col.getMaxStrLength()), BsonType.STRING, col.getContainValCount(), false));
                    }
                }
            }
        }
        result.sort((c1, c2) -> c1.getIndex() - c2.getIndex());
        return result;
    }

    public void incrContainValCount() {
        this.containValCount++;
    }

    public int getContainValCount() {
        return containValCount;
    }

    private static DataType mapType(BsonType mongoFieldType) {
        switch (mongoFieldType) {
            case NULL:
                return null;
            case INT32:
                return DataType.getType(JDBCTypes.INTEGER);
            case INT64:
                return DataType.getType(JDBCTypes.BIGINT);
            case BINARY:
                return DataType.getType(JDBCTypes.BINARY);
            case DOUBLE:
                return DataType.getType(JDBCTypes.DOUBLE);
            case BOOLEAN:
                return DataType.getType(JDBCTypes.BOOLEAN);
            case MAX_KEY:
            case MIN_KEY:
            case OBJECT_ID:
            case UNDEFINED:
            case DB_POINTER:
            case STRING:
                return DataType.getType(JDBCTypes.VARCHAR);
            case DATE_TIME:
                return DataType.getType(JDBCTypes.DATE);
            case TIMESTAMP:
                return DataType.getType(JDBCTypes.TIMESTAMP);
            case DECIMAL128:
                return DataType.getType(JDBCTypes.DECIMAL);
            case JAVASCRIPT:
            case END_OF_DOCUMENT:
            case REGULAR_EXPRESSION:
            case JAVASCRIPT_WITH_SCOPE:
            case SYMBOL:
            case DOCUMENT:
            case ARRAY:
                return DataType.getType(JDBCTypes.LONGVARCHAR);
            default:
                throw new IllegalStateException("illega type:" + mongoFieldType);
        }
    }

    public BsonType getMongoFieldType() {
        return this.mongoFieldType;
    }

    public void setMaxStrLength(int length) {
        this.maxStrLength = Math.max(length, this.maxStrLength);
    }

    @Override
    protected CMeta createCmeta() {
        MongoCMeta cMeta = new MongoCMeta();
        cMeta.setMongoFieldType(this.mongoFieldType);
        if (cMeta.isMongoDocType()) {
            cMeta.setFlatMapDocumentTypes(flatMapMongoDocument());
        }
        return cMeta;
    }

    @JSONField(serialize = false)
    public int getMaxStrLength() {
        return maxStrLength;
    }

    private List<ColumnMetaData> flatMapMongoDocument() {
        if (!(this.mongoFieldType == BsonType.DOCUMENT)) {
            throw new IllegalStateException("execute docuemtn field flatmap but this field is not document type,is:" + this.mongoFieldType);
        }

        Map<String, MongoColumnMetaData> colsSchema = this.docTypeFieldEnum;
        return reorder(colsSchema).stream() //
                .filter((c) -> ((MongoColumnMetaData) c).mongoFieldType != BsonType.DOCUMENT).collect(Collectors.toList());

    }

}
