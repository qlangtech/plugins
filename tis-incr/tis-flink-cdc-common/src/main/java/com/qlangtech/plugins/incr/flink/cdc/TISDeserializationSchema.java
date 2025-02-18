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

package com.qlangtech.plugins.incr.flink.cdc;

import com.qlangtech.tis.plugin.ds.RdbmsRunningContext;
import com.qlangtech.tis.plugin.ds.RunningContext;
import com.qlangtech.tis.realtime.transfer.DTO;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * A JSON format implementation of {@link DebeziumDeserializationSchema} which deserializes the
 * received {@link SourceRecord} to JSON String.
 *
 * @see org.apache.flink.cdc.debezium.table.RowDataDebeziumDeserializeSchema
 */
public class TISDeserializationSchema implements DebeziumDeserializationSchema<DTO> {
    private static final Pattern PATTERN_TOPIC = Pattern.compile(".+\\.(.+)\\.(.+)");
    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(TISDeserializationSchema.class);

    protected final ISourceValConvert rawValConvert;
    private final Function<String, String> physicsTabName2LogicName;
    private final Map<String /**tableName*/, Map<String, Function<RunningContext, Object>>> contextParamValsGetterMapper;

    public TISDeserializationSchema(ISourceValConvert rawValConvert, Function<String, String> physicsTabName2LogicName
            , Map<String /*tableName*/, Map<String, Function<RunningContext, Object>>> contextParamValsGetterMapper) {
        this.rawValConvert = rawValConvert;
        this.physicsTabName2LogicName = physicsTabName2LogicName;
        this.contextParamValsGetterMapper = contextParamValsGetterMapper;
    }

    public TISDeserializationSchema() {
        this(new DefaultSourceValConvert(), new DefaultTableNameConvert(), Collections.emptyMap());
    }

//    public TISDeserializationSchema(boolean includeSchema) {
//        final HashMap<String, Object> configs = new HashMap<>();
//        configs.put(ConverterConfig.TYPE_CONFIG, ConverterType.VALUE.getName());
//        configs.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, includeSchema);
//        CONVERTER.configure(configs);
//    }

    @Override
    public void deserialize(SourceRecord record, Collector<DTO> out) throws Exception {
        DTO dto = new DTO();
        EventOperation op = getOp(record);
        if (isSkipProcess(op)) {
            return;
        }
        Struct value = (Struct) record.value();
        Schema valueSchema = record.valueSchema();
        setTableRelevantMeta(record, dto);

        dto.setTableName(physicsTabName2LogicName.apply(dto.getPhysicsTabName()));

        if (op.isInsert()) {
            if (this.extractAfterRow(op, dto, value, valueSchema)) {
                this.processNewAdd(out, dto);
            }
        } else if (op.isUpdate()) {
            boolean containBeforeVals = this.extractBeforeRow(dto, value, valueSchema);
            boolean containAfterVals = this.extractAfterRow(op, dto, value, valueSchema);

            if (containBeforeVals) {
                dto.setEventType(DTO.EventType.UPDATE_BEFORE);
                out.collect(dto);
            }
            if (containAfterVals) {
                dto = dto.clone();
                dto.setEventType(DTO.EventType.UPDATE_AFTER);
                out.collect(dto);
            }
        } else if (op.isDelete()) {

            this.extractForDelete(dto, value, valueSchema);
            dto.setEventType(DTO.EventType.DELETE);
            out.collect(dto);
        } else {
            throw new IllegalStateException("invalid op:" + op);
        }

//        if (op != Envelope.Operation.CREATE && op != Envelope.Operation.READ) {
//            if (op == Envelope.Operation.DELETE) {
//                this.extractBeforeRow(dto, value, valueSchema);
//                dto.setEventType(DTO.EventType.DELETE);
//                out.collect(dto);
//            } else {
//                this.extractBeforeRow(dto, value, valueSchema);
//                this.extractAfterRow(dto, value, valueSchema);
//
//                processUpdate(out, dto);
//            }
//        } else {
//            this.extractAfterRow(dto, value, valueSchema);
//            processNewAdd(out, dto);
//        }
    }

    protected void extractForDelete(DTO dto, Struct value, Schema valueSchema) {
        this.extractBeforeRow(dto, value, valueSchema);
    }

    protected boolean isSkipProcess(EventOperation op) {
        return false;
    }

    protected EventOperation getOp(SourceRecord record) {
        return new EventOperation(Envelope.operationFor(record));
    }

    protected void setTableRelevantMeta(SourceRecord record, DTO dto) {
        Matcher topicMatcher = PATTERN_TOPIC.matcher(record.topic());
        if (!topicMatcher.matches()) {
            throw new IllegalStateException("topic is illegal:" + record.topic());
        }
        dto.setDbName(topicMatcher.group(1));
        final String physicsTabName = topicMatcher.group(2);
        dto.setPhysicsTabName(physicsTabName);
    }

    protected void processNewAdd(Collector<DTO> out, DTO dto) {
        dto.setEventType(DTO.EventType.ADD);
        out.collect(dto);
    }

    /**
     * @param dto
     * @param value
     * @param valueSchema
     * @return 是否收集
     */
    protected boolean extractBeforeRow(DTO dto, Struct value, Schema valueSchema) {
        final List<Field> fields = getBeforeFields(dto, value, valueSchema);
        Struct before = getBeforeVal(value);
        Map<String, Object> beforeVals = createInitVals(dto);// new HashMap<>();
        if (before != null) {
            Object beforeVal = null;
            for (Field f : fields) {
                beforeVal = before.get(f.name());
                if (beforeVal == null) {
                    continue;
                }
                try {
                    beforeVals.put(f.name(), rawValConvert.convert(dto, f, beforeVal));
                } catch (Exception e) {
                    throw new RuntimeException("field:" + f.name() + ",beforeVal:" + beforeVal, e);
                }
            }
        }
        dto.setBefore(beforeVals);
        return true;
    }


    private boolean extractAfterRow(EventOperation operation, DTO dto, Struct value, Schema valueSchema) {
        final List<Field> fields = getAfterFields(dto, value, valueSchema);

        Map<String, Object> afterVals = createInitVals(dto);

        if (fillAfterValsFromEvent(operation, dto, value, valueSchema, fields, afterVals)) {
            dto.setAfter(afterVals);
            return true;
        } else {
            return false;
        }
    }

    private Map<String, Object> createInitVals(DTO dto) {
        Map<String, Object> vals = new HashMap<>();

        /**==========================
         * 设置环境绑定参数值
         ==========================*/
        Map<String, Function<RunningContext, Object>> contextParamsGetter = this.contextParamValsGetterMapper.get(dto.getTableName());
        if (contextParamsGetter != null) {
            RdbmsRunningContext runningParamsContext = new RdbmsRunningContext(dto.getDbName(), dto.getPhysicsTabName());
            contextParamsGetter.forEach((contextParamName, getter) -> {
                vals.put(contextParamName, getter.apply(runningParamsContext));
            });
        }
        return vals;
    }

    protected boolean fillAfterValsFromEvent(EventOperation operation, DTO dto, Struct value
            , Schema valueSchema, List<Field> fields, Map<String, Object> afterVals) {
        Object afterVal = null;
        Struct after = value.getStruct("after");
        for (Field field : fields) {
            afterVal = after.get(field);
            if (afterVal == null) {
                continue;
            }
            try {
                afterVals.put(field.name(), rawValConvert.convert(dto, field, afterVal));
            } catch (Exception e) {
                throw new RuntimeException("field:" + field.name() + ",afterVal:" + afterVal, e);
            }
        }
        return true;
    }


    protected List<Field> getAfterFields(DTO dto, Struct value, Schema valueSchema) {
        Schema afterSchema = valueSchema.field("after").schema();
        return afterSchema.fields();
    }

    protected List<Field> getBeforeFields(DTO dto, Struct value, Schema valueSchema) {
        Schema afterSchema = valueSchema.field("before").schema();
        return afterSchema.fields();
    }




    protected Struct getBeforeVal(Struct value) {
        return value.getStruct("before");
    }

    @Override
    public TypeInformation<DTO> getProducedType() {
        return TypeInformation.of(DTO.class);
    }
}
