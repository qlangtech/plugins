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
//package com.qlangtech.tis.plugin.ontology.impl.action.widget.impl;
//
//import com.alibaba.fastjson.JSONObject;
//
//import com.qlangtech.tis.extension.TISExtension;
//import com.qlangtech.tis.plugin.annotation.FormField;
//import com.qlangtech.tis.plugin.annotation.FormFieldType;
//import com.qlangtech.tis.plugin.ontology.impl.action.widget.UserInputWidgetType;
//
///**
// * 数据库列选择控件：先定位一张表，再从该表里选列。
// *
// * @author 百岁 (baisui@qlangtech.com)
// * @date 2026/10/15
// */
//public class JdbcColumnWidget extends UserInputWidgetType {
//
//    /**
//     * 提供列候选集的数据源引用；前端据此拉取该数据源下的表 / 列清单。
//     */
//    @FormField(ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {})
//    public String dataSourceRef;
//
//    @Override
//    public FormFieldType getFormFieldType() {
//        return FormFieldType.JDBCColumn;
//    }
//
//    @Override
//    public Object getDefaultValue() {
//        return null;
//    }
//
//    @Override
//    protected void appendExtraPropsJson(JSONObject fieldJson) {
//    }
//
//    @TISExtension
//    public static class DftDescriptor extends BasicDescriptor {
//
//        @Override
//        public FormFieldType getFormFieldType() {
//            return FormFieldType.JDBCColumn;
//        }
//
//        @Override
//        public String getDisplayName() {
//            return "Jdbc_Column";
//        }
//    }
//}
