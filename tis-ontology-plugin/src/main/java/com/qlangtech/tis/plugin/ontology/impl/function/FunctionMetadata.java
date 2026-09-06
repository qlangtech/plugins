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

package com.qlangtech.tis.plugin.ontology.impl.function;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.extension.OneStepOfMultiSteps;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.manage.common.Option;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import com.qlangtech.tis.util.IPluginContext;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * Function Step 1: 元数据配置
 *
 * 定义 Function 的基本信息：名称、显示名、描述、语言、函数类型
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/6
 */
public class FunctionMetadata extends OneStepOfMultiSteps {

    @FormField(ordinal = 0, validate = {Validator.require, Validator.identity})
    public String name;

    @FormField(ordinal = 1, validate = {Validator.require})
    public String displayName;

    @FormField(ordinal = 2, type = FormFieldType.TEXTAREA, validate = {Validator.require})
    public String description;

    @FormField(ordinal = 3, type = FormFieldType.ENUM, validate = {Validator.require})
    public String language;

    @FormField(ordinal = 4, type = FormFieldType.ENUM, validate = {Validator.require})
    public String functionType;

    @Override
    public void processPreSaved(IPluginContext pluginContext, Context currentCtx,
                               OneStepOfMultiSteps[] preSavedStepPlugins) {
        // 保存当前步骤数据到上下文，供后续步骤使用
        currentCtx.put(FunctionMetadata.class.getName(), this);
        super.processPreSaved(pluginContext, currentCtx, preSavedStepPlugins);
    }

    @TISExtension
    public static class Desc extends OneStepOfMultiSteps.BasicDesc {

        @Override
        public String getStepDescription() {
            return "Metadata";
        }

        @Override
        public Step getStep() {
            return Step.Step1;
        }

        @Override
        public Optional<BasicDesc> nextPluginDesc(OneStepOfMultiSteps current) {
            return Optional.of(new FunctionImplementation.Desc());
        }

        @Override
        public boolean isFinalStep() {
            return false;
        }

        /**
         * 验证 name 字段
         */
        public boolean validateName(IFieldErrorHandler msgHandler, Context context,
                                   String fieldName, String value) {
            if (StringUtils.isEmpty(value)) {
                msgHandler.addFieldError(context, fieldName, "Function 名称不能为空");
                return false;
            }

            // 验证命名规范：只允许字母、数字、下划线
            if (!value.matches("^[a-zA-Z][a-zA-Z0-9_]*$")) {
                msgHandler.addFieldError(context, fieldName,
                    "Function 名称必须以字母开头，只能包含字母、数字和下划线");
                return false;
            }

            return true;
        }

        /**
         * 验证 displayName 字段
         */
        public boolean validateDisplayName(IFieldErrorHandler msgHandler, Context context,
                                          String fieldName, String value) {
            if (StringUtils.isEmpty(value)) {
                msgHandler.addFieldError(context, fieldName, "显示名称不能为空");
                return false;
            }
            return true;
        }

        /**
         * 验证 description 字段
         */
        public boolean validateDescription(IFieldErrorHandler msgHandler, Context context,
                                          String fieldName, String value) {
            if (StringUtils.isEmpty(value)) {
                msgHandler.addFieldError(context, fieldName, "描述不能为空");
                return false;
            }
            return true;
        }

        /**
         * 获取 language 的可选项
         * 根据用户需求，只支持 Groovy
         */
        public List<Option> getLanguageOptions() {
            return Arrays.asList(
                new Option("GROOVY", "Groovy")
            );
        }

        /**
         * 验证 language 字段
         */
        public boolean validateLanguage(IFieldErrorHandler msgHandler, Context context,
                                       String fieldName, String value) {
            if (StringUtils.isEmpty(value)) {
                msgHandler.addFieldError(context, fieldName, "编程语言不能为空");
                return false;
            }

            if (!value.equals("GROOVY")) {
                msgHandler.addFieldError(context, fieldName, "当前仅支持 Groovy 语言");
                return false;
            }

            return true;
        }

        /**
         * 获取 functionType 的可选项
         */
        public List<Option> getFunctionTypeOptions() {
            return Arrays.asList(
                new Option("QUERY", "Query - 只读查询函数"),
                new Option("ONTOLOGY_EDIT", "Ontology Edit - 写入函数")
            );
        }

        /**
         * 验证 functionType 字段
         */
        public boolean validateFunctionType(IFieldErrorHandler msgHandler, Context context,
                                           String fieldName, String value) {
            if (StringUtils.isEmpty(value)) {
                msgHandler.addFieldError(context, fieldName, "函数类型不能为空");
                return false;
            }

            if (!value.equals("QUERY") && !value.equals("ONTOLOGY_EDIT")) {
                msgHandler.addFieldError(context, fieldName, "无效的函数类型");
                return false;
            }

            return true;
        }
    }
}
