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
//package com.qlangtech.tis.realtime;
//
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.util.OutputTag;
//
//
//import java.util.Map;
//
///**
// * 获取kafka流标记名称
// *
// * @author: 百岁（baisui@qlangtech.com）
// * @create: 2025-01-04 09:41
// **/
//public class KafkaSourceTagProcessFunction extends SourceProcessFunction<Tuple2<String, byte[]>> {
//    public KafkaSourceTagProcessFunction(Map<String, OutputTag<Tuple2<String, byte[]>>> tab2OutputTag) {
//        super(tab2OutputTag);
//    }
//
//    @Override
//    protected String getTableName(Tuple2<String, byte[]> record) {
//        return record.f0;
//    }
//}
