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
//package com.qlangtech.tis.plugin.datax.doris.datamodel;
//
//import com.qlangtech.tis.extension.Descriptor;
//import com.qlangtech.tis.extension.TISExtension;
//import com.qlangtech.tis.plugin.datax.doris.CreateTable;
//
///**
// * @author: 百岁（baisui@qlangtech.com）
// * @create: 2024-02-28 10:30
// **/
//public class OffCreateTable extends CreateTable {
//    @Override
//    public String getKeyToken() {
//        throw new UnsupportedOperationException();
//    }
//
//    @Override
//    public boolean isOff() {
//        return true;
//    }
//
//    @TISExtension
//    public static class DftDesc extends Descriptor<CreateTable> {
//        @Override
//        public String getDisplayName() {
//            return SWITCH_OFF;
//        }
//    }
//}
