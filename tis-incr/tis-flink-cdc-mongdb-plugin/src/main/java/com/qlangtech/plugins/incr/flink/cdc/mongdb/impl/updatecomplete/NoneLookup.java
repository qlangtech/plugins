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
//package com.qlangtech.plugins.incr.flink.cdc.mongdb.impl.updatecomplete;
//
//import com.qlangtech.plugins.incr.flink.cdc.mongdb.UpdateRecordComplete;
//import com.qlangtech.tis.extension.Descriptor;
//import com.qlangtech.tis.extension.TISExtension;
//import com.qlangtech.tis.realtime.transfer.DTO;
//import org.apache.flink.cdc.connectors.mongodb.MongoDBSource.Builder;
//
///**
// * 不获取mongo增量before值
// *
// * @author: 百岁（baisui@qlangtech.com）
// * @create: 2024-12-08 09:26
// **/
//public class NoneLookup extends UpdateRecordComplete {
//    @Override
//    public void setProperty(Builder<DTO> builder) {
//        builder.scanFullChangelog(false);
//        builder.updateLookup(false);
//    }
//
//    @TISExtension
//    public static final class DftDescriptor extends Descriptor<UpdateRecordComplete> {
//        public DftDescriptor() {
//            super();
//        }
//
//        @Override
//        public String getDisplayName() {
//            return "NONE_LOOKUP";
//        }
//    }
//}
