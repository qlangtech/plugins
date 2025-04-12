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
//package com.qlangtech.tis.plugins.incr.flink.chunjun.common;
//
//import com.dtstack.chunjun.connector.jdbc.sink.IFieldNamesAttachedStatement;
//import com.dtstack.chunjun.converter.ISerializationConverter;
//import com.qlangtech.plugins.incr.flink.cdc.FlinkCol;
//import org.apache.flink.table.types.logical.LogicalType;
//
///**
// * @author: 百岁（baisui@qlangtech.com）
// * @create: 2025-04-11 16:45
// **/
//public class ExternalConverter {
//    private final ISerializationConverter<IFieldNamesAttachedStatement> serConverter;
//    private final FlinkCol flinkCol;
//
//    public ExternalConverter(ISerializationConverter<IFieldNamesAttachedStatement> serConverter, FlinkCol flinkCol) {
//        this.serConverter = serConverter;
//        this.flinkCol = flinkCol;
//    }
//
//    public LogicalType getFlinkLogicalType() {
//
//        return this.flinkCol.type.getLogicalType();
//    }
//
//    public com.qlangtech.tis.plugin.ds.DataType getDataType() {
//        return this.flinkCol.colType;
//    }
//}
