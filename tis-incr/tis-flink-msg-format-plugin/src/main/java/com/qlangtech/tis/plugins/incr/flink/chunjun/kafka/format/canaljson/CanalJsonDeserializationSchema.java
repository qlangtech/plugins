///**
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// * <p>
// * http://www.apache.org/licenses/LICENSE-2.0
// * <p>
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package com.qlangtech.tis.plugins.incr.flink.chunjun.kafka.format.canaljson;
//
//import com.qlangtech.tis.realtime.transfer.DTO;
//import org.apache.flink.api.common.serialization.DeserializationSchema;
//import org.apache.flink.api.common.typeinfo.TypeInformation;
//import org.apache.flink.formats.json.JsonRowDataDeserializationSchema;
//import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
//import org.apache.flink.table.data.RowData;
//import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
//import org.apache.flink.table.types.logical.RowType;
//
//import java.io.IOException;
//import java.util.Collections;
//
///**
// * @author: 百岁（baisui@qlangtech.com）
// * @create: 2024-12-28 13:53
// **/
//public class CanalJsonDeserializationSchema implements DeserializationSchema<DTO> {
//
//    private final JsonRowDataDeserializationSchema jsonDeserializer;
//
//    public CanalJsonDeserializationSchema(boolean ignoreParseErrors) {
//        RowType rowType = new RowType(Collections.emptyList());
//        TypeInformation<RowData> outputType = InternalTypeInfo.of(rowType);
//        this.jsonDeserializer =
//                new JsonRowDataDeserializationSchema(
//                        rowType,
//                        // the result type is never used, so it's fine to pass in the produced type
//                        // info
//                        outputType,
//                        false, // ignoreParseErrors already contains the functionality of
//                        // failOnMissingField
//                        ignoreParseErrors,
//                        null);
//
//    }
//
//    @Override
//    public DTO deserialize(byte[] message) throws IOException {
//        DTO dto = new DTO();
//        JsonNode root = this.jsonDeserializer.deserializeToJsonNode(message);
////        dto.setTableName(root.get(ReadableMetadata.TABLE.key).asText());
////        dto.setPhysicsTabName();
//        return dto;
//    }
//
//    @Override
//    public boolean isEndOfStream(DTO nextElement) {
//        return false;
//    }
//
//    @Override
//    public TypeInformation<DTO> getProducedType() {
//        return TypeInformation.of(DTO.class);
//    }
//}
