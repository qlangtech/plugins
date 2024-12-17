/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.qlangtech.plugins.incr.flink.cdc.mongdb.impl;

import com.alibaba.datax.common.element.Column;
import com.mongodb.client.model.changestream.OperationType;
import com.qlangtech.plugins.incr.flink.cdc.EventOperation;
import com.qlangtech.plugins.incr.flink.cdc.ISourceValConvert;
import com.qlangtech.plugins.incr.flink.cdc.TISDeserializationSchema;
import com.qlangtech.plugins.incr.flink.cdc.mongdb.MongoDBSourceDTOColValProcess;
import com.qlangtech.tis.plugin.datax.mongo.MongoCMeta;
import com.qlangtech.tis.plugin.ds.RunningContext;
import com.qlangtech.tis.realtime.transfer.DTO;
import io.debezium.data.Envelope.Operation;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.cdc.connectors.mongodb.internal.MongoDBEnvelope;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.bson.BsonDocument;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-12-06 11:17
 * @see org.apache.flink.cdc.connectors.mongodb.table.MongoDBConnectorDeserializationSchema 可以参考
 **/
public class MongoDBDeserializationSchema extends TISDeserializationSchema {
    private static final Pattern PATTERN_MONGO_TOPIC = Pattern.compile("(.+)\\.(.+)");
    private final MongoDBSourceDTOColValProcess colValProcess;

    public MongoDBDeserializationSchema(ISourceValConvert rawValConvert
            , Function<String, String> physicsTabName2LogicName
            , Map<String, Map<String, Function<RunningContext, Object>>> contextParamValsGetterMapper) {
        super(rawValConvert, physicsTabName2LogicName, contextParamValsGetterMapper);
        this.colValProcess = (MongoDBSourceDTOColValProcess) rawValConvert;
    }

    @Override
    protected boolean isSkipProcess(EventOperation op) {
        return (op == null);
    }

    @Override
    protected List<Field> getAfterFields(DTO dto, Struct value, Schema valueSchema) {
        return colValProcess.getFields(dto.getTableName());
    }

    @Override
    protected List<Field> getBeforeFields(DTO dto, Struct value, Schema valueSchema) {
        return this.getAfterFields(dto, value, valueSchema);
    }

    @Override
    protected void extractForDelete(DTO dto, Struct value, Schema valueSchema) {
        boolean containBeforeVals = this.extractBeforeRow(dto, value, valueSchema);
        if (!containBeforeVals) {
           // final List<Field> fields = getBeforeFields(dto, value, valueSchema);
            BsonDocument documentKey =
                    this.extractBsonDocument(value, valueSchema, MongoDBEnvelope.DOCUMENT_KEY_FIELD);
            Map<String, Object> beforeVals = new HashMap<>();
            Object beforeVal = null;
            String colName = null;
//            for (Field field : fields) {
//                beforeVal = documentKey.get(field.name());
//                if (beforeVal == null
//                        || beforeVal instanceof org.bson.BsonNull) {
//                    continue;
//                }
//                try {
//                    beforeVals.put(field.name(), rawValConvert.convert(dto, field, beforeVal));
//                } catch (Exception e) {
//                    throw new RuntimeException("field:" + field.name() + ",afterVal:" + beforeVal, e);
//                }
//            }

            for (Pair<MongoCMeta, Function<BsonDocument, Object>> colProducter
                    : colValProcess.getMongoColValProductor(dto.getTableName())) {
                beforeVal = colProducter.getValue().apply(documentKey);
                if (beforeVal == null) {
                    continue;
                }
                colName = colProducter.getKey().getName();
                try {
                    beforeVals.put(colName, beforeVal);
                } catch (Exception e) {
                    throw new RuntimeException("field:" + colName + ",beforeVal:" + beforeVal, e);
                }
            }

            dto.setBefore(beforeVals);
        }
    }

    @Override
    protected boolean extractBeforeRow(DTO dto, Struct value, Schema valueSchema) {
        final List<Field> fields = getBeforeFields(dto, value, valueSchema);
        BsonDocument fullDocumentBeforeChange =
                extractBsonDocument(
                        value, valueSchema, MongoDBEnvelope.FULL_DOCUMENT_BEFORE_CHANGE_FIELD);

        Object beforeVal = null;
        Map<String, Object> beforeVals = new HashMap<>();
        dto.setBefore(beforeVals);
        String colName = null;
        if (fullDocumentBeforeChange != null) {

//            for (Field field : fields) {
//                beforeVal = fullDocumentBeforeChange.get(field.name()); // after.get(field);
//                if (beforeVal == null
//                        || beforeVal instanceof org.bson.BsonNull) {
//                    continue;
//                }
//                try {
//                    beforeVals.put(field.name(), rawValConvert.convert(dto, field, beforeVal));
//                } catch (Exception e) {
//                    throw new RuntimeException("field:" + field.name() + ",afterVal:" + beforeVal, e);
//                }
//            }


            for (Pair<MongoCMeta, Function<BsonDocument, Object>> colProducter
                    : colValProcess.getMongoColValProductor(dto.getTableName())) {
                beforeVal = colProducter.getValue().apply(fullDocumentBeforeChange);
                if (beforeVal == null) {
                    continue;
                }
                colName = colProducter.getKey().getName();
                try {
                    beforeVals.put(colName, beforeVal);
                } catch (Exception e) {
                    throw new RuntimeException("field:" + colName + ",beforeVal:" + beforeVal, e);
                }
            }


            return true;
        }

        return false;
//        Struct before = getBeforeVal(value);
//        if (before != null) {
//            Map<String, Object> beforeVals = new HashMap<>();
//            Object beforeVal = null;
//            for (Field f : fields) {
//                beforeVal = before.get(f.name());
//                if (beforeVal == null) {
//                    continue;
//                }
//                try {
//                    beforeVals.put(f.name(), rawValConvert.convert(dto, f, beforeVal));
//                } catch (Exception e) {
//                    throw new RuntimeException("field:" + f.name() + ",beforeVal:" + beforeVal, e);
//                }
//            }
//            dto.setBefore(beforeVals);
//        }
    }

    @Override
    protected boolean fillAfterValsFromEvent(EventOperation operation, DTO dto, Struct value
            , Schema valueSchema, List<Field> fields, Map<String, Object> afterVals) {
//        BsonDocument documentKey =
//                extractBsonDocument(value, valueSchema, MongoDBEnvelope.DOCUMENT_KEY_FIELD);
        BsonDocument fullDocument =
                extractBsonDocument(value, valueSchema, MongoDBEnvelope.FULL_DOCUMENT_FIELD);
        OperationType mongoOperationType = operation.getPayload1();
        if (mongoOperationType != null
                && mongoOperationType == OperationType.UPDATE) {
            if (fullDocument == null) {
                // It’s null if another operation deletes the document
                // before the lookup operation happens. Ignored it.
                // 需要直接跳过，不处理
                return false;
            }
        }


        // Struct after = value.getStruct("after");
//        for (Field field : fields) {
//            afterVal = fullDocument.get(field.name()); // after.get(field);
//            if (afterVal == null
//                    || afterVal instanceof org.bson.BsonNull) {
//                continue;
//            }
//            try {
//                afterVals.put(field.name(), rawValConvert.convert(dto, field, afterVal));
//            } catch (Exception e) {
//                throw new RuntimeException("field:" + field.name() + ",afterVal:" + afterVal, e);
//            }
//        }

//        for (Pair<MongoCMeta, Function<BsonDocument, Column>> p : this.getMongoPresentCols()) {
//            record.addColumn(p.getValue().apply(item));
//        }
        Object afterVal = null;
        String colName = null;
        for (Pair<MongoCMeta, Function<BsonDocument, Object>> colProducter
                : colValProcess.getMongoColValProductor(dto.getTableName())) {
            afterVal = colProducter.getValue().apply(fullDocument);
            if (afterVal == null) {
                continue;
            }
            colName = colProducter.getKey().getName();
            try {
                afterVals.put(colName, afterVal);
            } catch (Exception e) {
                throw new RuntimeException("field:" + colName + ",afterVal:" + afterVal, e);
            }
        }

        return true;
    }

    protected BsonDocument extractBsonDocument(Struct value, Schema valueSchema, String fieldName) {
        if (valueSchema.field(fieldName) != null) {
            String docString = value.getString(fieldName);
            if (docString != null) {
                return BsonDocument.parse(docString);
            }
        }
        return null;
    }


    @Override
    protected void setTableRelevantMeta(SourceRecord record, DTO dto) {
        Matcher topicMatcher = PATTERN_MONGO_TOPIC.matcher(record.topic());
        if (!topicMatcher.matches()) {
            throw new IllegalStateException("topic is illegal:" + record.topic());
        }
        dto.setDbName(topicMatcher.group(1));
        final String physicsTabName = topicMatcher.group(2);
        dto.setPhysicsTabName(physicsTabName);
    }

    @Override
    protected EventOperation getOp(SourceRecord record) {
        Struct value = (Struct) record.value();
        OperationType operationType = OperationType.fromString(value.getString(MongoDBEnvelope.OPERATION_TYPE_FIELD));
        switch (operationType) {
            case INSERT:
                return new EventOperation(Operation.CREATE);
            case DELETE:
                return new EventOperation(Operation.DELETE);
            case UPDATE:
            case REPLACE:
                return new EventOperation(Operation.UPDATE).setPayload1(operationType);
            case DROP:
            case OTHER:
            case RENAME:
            case INVALIDATE:
            case DROP_DATABASE:
            default: {
                return null;
            }
        }
    }


}
