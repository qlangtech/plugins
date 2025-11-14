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

package com.qlangtech.tis.plugin.kafka.consumer;

import com.qlangtech.tis.plugin.datax.format.guesstype.KafkaLogicalTableName;
import com.qlangtech.tis.plugin.datax.format.guesstype.PhysicsTable2LogicalTableMapper;
import com.qlangtech.tis.plugin.ds.RdbmsRunningContext;
import com.qlangtech.tis.plugin.ds.RunningContext;
import com.qlangtech.tis.plugin.ds.RunningContext.RunningContextParamSetter;
import com.qlangtech.tis.plugins.incr.flink.chunjun.kafka.format.FormatFactory;
import com.qlangtech.tis.realtime.transfer.DTO;
import com.qlangtech.tis.realtime.transfer.DTO.EventType;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * kafka 中获得的消息，需要先经过一次分流，然后再使用 flink 提供的 deserialize解析成rowdata
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-12-27 13:56
 * @see org.apache.flink.formats.json.canal.CanalJsonDeserializationSchema
 **/
public class KafkaDTODeserializationSchema implements DeserializationSchema<DTO> {
    private final FormatFactory msgFormat;

    private final KafkaStructuredRecord reuseReocrd;
    private final Map<String /**tableName*/, RunningContextParamSetter> contextParamValsGetterMapper;
    private final PhysicsTable2LogicalTableMapper physicsTable2LogicalTableMapper;

    public KafkaDTODeserializationSchema(FormatFactory msgFormat
            , Map<String /**tableName*/, Map<String, Function<RunningContext, Object>>> contextParamValsGetterMapper) {
        this.msgFormat = Objects.requireNonNull(msgFormat, "msgFormat can not be null");
        this.physicsTable2LogicalTableMapper = new PhysicsTable2LogicalTableMapper(msgFormat.parseTargetTabsEntities());
        this.reuseReocrd = new KafkaStructuredRecord();
        this.contextParamValsGetterMapper =
                Objects.requireNonNull(contextParamValsGetterMapper, "contextParamValsGetterMapper can not be null")
                        .entrySet().stream().collect(
                                Collectors.toMap(
                                        (entry) -> entry.getKey()
                                        , (entry) -> new RunningContextParamSetter(new RdbmsRunningContext(StringUtils.EMPTY, entry.getKey()), entry.getValue())
                                        , (u, v) -> {
                                            throw new IllegalStateException(String.format("Duplicate key %s", u));
                                        }
                                        , () -> new TreeMap<>(String.CASE_INSENSITIVE_ORDER)
                                ));
    }

    @Override
    public DTO deserialize(byte[] message) throws IOException {
        throw new UnsupportedEncodingException("please use deserialize(byte[] message, Collector<DTO> out) instead");
    }

    @Override
    public void deserialize(byte[] message, Collector<DTO> out) throws IOException {
        DTO dto = null;
        KafkaStructuredRecord record = (KafkaStructuredRecord) msgFormat.parseRecord(reuseReocrd, message);
        if (record == null) {
            return;
        }
        dto = new DTO();
        final KafkaLogicalTableName logicalTableName
                = physicsTable2LogicalTableMapper.parseLogicalTableName(record.tabName);
        dto.setTableName(logicalTableName.getLogicalTableName());
        EventType event = record.getEventType();
        dto.setEventType(event);

        if (event == null) {
            throw new IllegalStateException("event can not be null");
        }
        Map<String, Object> afterVals = getAfterVals(logicalTableName, record.vals);
        Map<String, Object> beforeVals = record.getOldVals();
        switch (event) {
            case DELETE: {
                dto.setBefore(beforeVals);
                out.collect(dto);
                break;
            }
            case ADD: {
                dto.setAfter(afterVals);
                out.collect(dto);
                break;
            }
            case UPDATE_AFTER: {

                dto.setAfter(afterVals);
                dto.setBefore(beforeVals);
                dto.setEventType(EventType.UPDATE_AFTER);
                out.collect(dto);

                out.collect(dto.clone().setEventType(EventType.UPDATE_BEFORE));
                break;
            }
            default:
                throw new IllegalStateException("unsupport event:" + event);
        }


//        dto.setBefore(beforeVals);
//        dto.setAfter(afterVals);

        // return Tuple2.of(record.tabName, message);
        //  return dto;
    }

    protected Map<String, Object> getAfterVals(KafkaLogicalTableName tabName, Map<String, Object> vals) {

        /**==========================
         * 设置环境绑定参数值
         ==========================*/
        RunningContextParamSetter contextParamsGetter = this.contextParamValsGetterMapper.get(tabName.getLogicalTableName());
        if (contextParamsGetter != null) {
            // RdbmsRunningContext runningParamsContext = new RdbmsRunningContext(StringUtils.EMPTY, tabName);
            contextParamsGetter.setContextParam(vals);

//                    .getValue().forEach((contextParamName, getter) -> {
//                vals.put(contextParamName, getter.apply(contextParamsGetter.getKey()));
//            });
        }
        return vals;
    }

    @Override
    public boolean isEndOfStream(DTO nextElement) {
        return false;
    }

    @Override
    public TypeInformation<DTO> getProducedType() {
        // return new TupleTypeInfo<>(Types.STRING, Types.PRIMITIVE_ARRAY(TypeInformation.of(Byte.TYPE)));
        return TypeInformation.of(DTO.class);
    }
}
