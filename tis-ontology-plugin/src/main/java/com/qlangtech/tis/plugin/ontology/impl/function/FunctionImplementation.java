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
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.extension.OneStepOfMultiSteps;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import com.qlangtech.tis.util.IPluginContext;
import org.apache.commons.lang3.StringUtils;

import java.util.Optional;

/**
 * Function Step 2: 实现配置
 *
 * 定义 Function 的实现细节：输入参数、返回类型、实现代码、测试用例
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/6
 */
public class FunctionImplementation extends OneStepOfMultiSteps {

    @FormField(ordinal = 0, type = FormFieldType.TEXTAREA, validate = {Validator.require})
    public String inputParameters;

    @FormField(ordinal = 1, validate = {Validator.require})
    public String returnType;

    @FormField(ordinal = 2, type = FormFieldType.TEXTAREA, validate = {Validator.require})
    public String implementation;

    @FormField(ordinal = 3, type = FormFieldType.TEXTAREA, validate = {})
    public String testCases;

    @Override
    public void processPreSaved(IPluginContext pluginContext, Context currentCtx,
                               OneStepOfMultiSteps[] preSavedStepPlugins) {
        // 保存当前步骤数据到上下文
        currentCtx.put(FunctionImplementation.class.getName(), this);
        super.processPreSaved(pluginContext, currentCtx, preSavedStepPlugins);
    }

    @TISExtension
    public static class Desc extends OneStepOfMultiSteps.BasicDesc {

        @Override
        public String getStepDescription() {
            return "Implementation";
        }

        @Override
        public Step getStep() {
            return Step.Step2;
        }

        @Override
        public Optional<BasicDesc> nextPluginDesc(OneStepOfMultiSteps current) {
            // 这是最后一步
            return Optional.empty();
        }

        @Override
        public boolean isFinalStep() {
            return true;
        }

        /**
         * 验证 inputParameters 字段
         * 确保是有效的 JSON 数组格式
         */
        public boolean validateInputParameters(IFieldErrorHandler msgHandler, Context context,
                                              String fieldName, String value) {
            if (StringUtils.isEmpty(value)) {
                msgHandler.addFieldError(context, fieldName, "输入参数配置不能为空");
                return false;
            }

            // 验证 JSON 格式
            try {
                JSONArray array = JSON.parseArray(value);
                if (array == null) {
                    msgHandler.addFieldError(context, fieldName, "输入参数配置必须是 JSON 数组");
                    return false;
                }

                // 可以是空数组（无参数函数）
                if (array.isEmpty()) {
                    return true;
                }

                // 验证每个参数对象的必需字段
                for (int i = 0; i < array.size(); i++) {
                    JSONObject param = array.getJSONObject(i);

                    if (!param.containsKey("name") || StringUtils.isEmpty(param.getString("name"))) {
                        msgHandler.addFieldError(context, fieldName,
                            "参数 #" + (i + 1) + " 缺少必需字段 'name'");
                        return false;
                    }

                    if (!param.containsKey("dataType") || StringUtils.isEmpty(param.getString("dataType"))) {
                        msgHandler.addFieldError(context, fieldName,
                            "参数 #" + (i + 1) + " 缺少必需字段 'dataType'");
                        return false;
                    }

                    // 验证参数名称格式
                    String paramName = param.getString("name");
                    if (!paramName.matches("^[a-zA-Z][a-zA-Z0-9_]*$")) {
                        msgHandler.addFieldError(context, fieldName,
                            "参数 #" + (i + 1) + " 的 name '" + paramName + "' 格式无效，必须以字母开头");
                        return false;
                    }

                    // 验证 dataType
                    String dataType = param.getString("dataType");
                    if (!isValidDataType(dataType)) {
                        msgHandler.addFieldError(context, fieldName,
                            "参数 #" + (i + 1) + " 的 dataType '" + dataType + "' 无效");
                        return false;
                    }
                }

                return true;

            } catch (Exception e) {
                msgHandler.addFieldError(context, fieldName,
                    "输入参数配置 JSON 格式错误: " + e.getMessage());
                return false;
            }
        }

        /**
         * 验证 returnType 字段
         */
        public boolean validateReturnType(IFieldErrorHandler msgHandler, Context context,
                                         String fieldName, String value) {
            if (StringUtils.isEmpty(value)) {
                msgHandler.addFieldError(context, fieldName, "返回类型不能为空");
                return false;
            }

            if (!isValidDataType(value)) {
                msgHandler.addFieldError(context, fieldName,
                    "返回类型 '" + value + "' 无效");
                return false;
            }

            return true;
        }

        /**
         * 验证 implementation 字段
         */
        public boolean validateImplementation(IFieldErrorHandler msgHandler, Context context,
                                             String fieldName, String value) {
            if (StringUtils.isEmpty(value)) {
                msgHandler.addFieldError(context, fieldName, "函数实现代码不能为空");
                return false;
            }

            // 可以添加基本的 Groovy 语法检查
            // 暂时只检查是否为空

            return true;
        }

        /**
         * 验证 testCases 字段（可选）
         */
        public boolean validateTestCases(IFieldErrorHandler msgHandler, Context context,
                                        String fieldName, String value) {
            // testCases 是可选的
            if (StringUtils.isEmpty(value)) {
                return true;
            }

            // 如果提供了值，验证 JSON 格式
            try {
                JSONArray array = JSON.parseArray(value);
                if (array == null) {
                    msgHandler.addFieldError(context, fieldName,
                        "测试用例必须是 JSON 数组");
                    return false;
                }

                // 验证每个测试用例的基本结构
                for (int i = 0; i < array.size(); i++) {
                    JSONObject testCase = array.getJSONObject(i);

                    if (!testCase.containsKey("name") || StringUtils.isEmpty(testCase.getString("name"))) {
                        msgHandler.addFieldError(context, fieldName,
                            "测试用例 #" + (i + 1) + " 缺少 'name' 字段");
                        return false;
                    }

                    if (!testCase.containsKey("inputs")) {
                        msgHandler.addFieldError(context, fieldName,
                            "测试用例 #" + (i + 1) + " 缺少 'inputs' 字段");
                        return false;
                    }
                }

                return true;

            } catch (Exception e) {
                msgHandler.addFieldError(context, fieldName,
                    "测试用例 JSON 格式错误: " + e.getMessage());
                return false;
            }
        }

        /**
         * 验证 dataType 是否有效
         */
        private boolean isValidDataType(String dataType) {
            return dataType != null && (
                dataType.equals("STRING") ||
                dataType.equals("INTEGER") ||
                dataType.equals("LONG") ||
                dataType.equals("DOUBLE") ||
                dataType.equals("DECIMAL") ||
                dataType.equals("BOOLEAN") ||
                dataType.equals("DATE") ||
                dataType.equals("TIMESTAMP") ||
                dataType.equals("OBJECT") ||
                dataType.equals("ARRAY") ||
                dataType.equals("STRUCT") ||
                dataType.equals("VOID")
            );
        }
    }
}
