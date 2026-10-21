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
// * 子插件列表控件（{@link FormFieldType#MULTI_DESCRIBLE_PLUGIN}）：
// * 用户在界面上反复增删一组子插件实例，每行的字段结构由 {@link #elementClass} 指向的
// * {@code Describable} 子类决定，表单形态则取该子插件 descriptor 自己的 viewStyle/colsConfig。
// *
// * @author 百岁 (baisui@qlangtech.com)
// * @date 2026/10/15
// */
//public class MultiDescriblePluginWidget extends UserInputWidgetType {
//
//    /**
//     * 子插件的 {@code Describable} 全限定名，如
//     * {@code com.qlangtech.tis.plugin.ontology.impl.action.PrimitiveDataType}。
//     */
//    @FormField(ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {})
//    public String elementClass;
//
//    /**
//     * 是否至少需要一行；默认要求至少一行，避免出现空列表这个无意义状态。
//     */
//    @FormField(ordinal = 1, type = FormFieldType.ENUM, validate = {})
//    public Boolean atLeastOne = true;
//
//    @FormField(ordinal = 2, type = FormFieldType.INT_NUMBER, validate = {})
//    public Integer maxCount;
//
//    @Override
//    public FormFieldType getFormFieldType() {
//        return FormFieldType.MULTI_DESCRIBLE_PLUGIN;
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
//            return FormFieldType.MULTI_DESCRIBLE_PLUGIN;
//        }
//
//        @Override
//        public String getDisplayName() {
//            return "Multi_Describle_Plugin";
//        }
//    }
//}
