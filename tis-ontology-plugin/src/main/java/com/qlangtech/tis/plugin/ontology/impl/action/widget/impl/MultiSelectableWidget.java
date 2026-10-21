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
//import com.qlangtech.tis.extension.Descriptor;
//import com.qlangtech.tis.extension.TISExtension;
//import com.qlangtech.tis.manage.common.Option;
//import com.qlangtech.tis.plugin.annotation.FormField;
//import com.qlangtech.tis.plugin.annotation.FormFieldType;
//import com.qlangtech.tis.plugin.annotation.SubForm;
//import com.qlangtech.tis.plugin.ontology.impl.action.widget.UserInputWidgetType;
//import com.qlangtech.tis.plugin.ontology.impl.action.widget.WidgetOptionConfig;
//
//import java.util.List;
//
///**
// * 多选控件。
// *
// * @author 百岁 (baisui@qlangtech.com)
// * @date 2026/10/15
// */
//public class MultiSelectableWidget extends UserInputWidgetType {
//
//    @SubForm(desClazz = WidgetOptionConfig.class, atLeastOne = false,
//            idListGetScript = "return java.util.Collections.emptyList();")
//    public List<WidgetOptionConfig> options;
//
//    @FormField(ordinal = 0, type = FormFieldType.INT_NUMBER, validate = {})
//    public Integer minSelect;
//
//    @FormField(ordinal = 1, type = FormFieldType.INT_NUMBER, validate = {})
//    public Integer maxSelect;
//
//    @Override
//    public FormFieldType getFormFieldType() {
//        return FormFieldType.MULTI_SELECTABLE;
//    }
//
//    @Override
//    public Object getDefaultValue() {
//        return null;
//    }
//
//    @Override
//    protected void appendExtraPropsJson(JSONObject fieldJson) {
//        if (options != null && !options.isEmpty()) {
//            fieldJson.put(Descriptor.KEY_ENUM_PROP, Option.toJson(toOptions(options)));
//        }
//    }
//
//    @TISExtension
//    public static class DftDescriptor extends BasicDescriptor {
//
//        @Override
//        public FormFieldType getFormFieldType() {
//            return FormFieldType.MULTI_SELECTABLE;
//        }
//
//        @Override
//        public String getDisplayName() {
//            return "Multi_Selectable";
//        }
//    }
//}
