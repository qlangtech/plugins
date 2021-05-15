///**
// * Copyright (c) 2020 QingLang, Inc. <baisui@qlangtech.com>
// * <p>
// * This program is free software: you can use, redistribute, and/or modify
// * it under the terms of the GNU Affero General Public License, version 3
// * or later ("AGPL"), as published by the Free Software Foundation.
// * <p>
// * This program is distributed in the hope that it will be useful, but WITHOUT
// * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
// * FITNESS FOR A PARTICULAR PURPOSE.
// * <p>
// * You should have received a copy of the GNU Affero General Public License
// * along with this program. If not, see <http://www.gnu.org/licenses/>.
// */
//
//package com.qlangtech.tis.plugin.oss;
//
//import com.qlangtech.tis.config.ParamsConfig;
//import com.qlangtech.tis.datax.impl.DataxReader;
//import com.qlangtech.tis.extension.Descriptor;
//import com.qlangtech.tis.extension.TISExtension;
//import com.qlangtech.tis.plugin.annotation.FormField;
//import com.qlangtech.tis.plugin.annotation.FormFieldType;
//import com.qlangtech.tis.plugin.annotation.Validator;
//
///**
// * @author: 百岁（baisui@qlangtech.com）
// * @create: 2021-05-13 13:08
// **/
//public class AliyunOssEndpoint extends ParamsConfig {
//    private static final String DATAX_NAME = "Oss-Endpoint";
//
//    @FormField(ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.identity})
//    public String name;
//    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
//    public String endpoint;
//    @FormField(ordinal = 2, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
//    public String accessId;
//    @FormField(ordinal = 3, type = FormFieldType.PASSWORD, validate = {Validator.require})
//    public String accessKey;
//
//    @Override
//    public <INSTANCE> INSTANCE createConfigInstance() {
//        return null;
//    }
//
//    @Override
//    public String identityValue() {
//        return null;
//    }
//
//    @TISExtension()
//    public static class DefaultDescriptor extends Descriptor<DataxReader> {
//        public DefaultDescriptor() {
//            super();
//        }
//
//        @Override
//        public String getDisplayName() {
//            return DATAX_NAME;
//        }
//    }
//}
