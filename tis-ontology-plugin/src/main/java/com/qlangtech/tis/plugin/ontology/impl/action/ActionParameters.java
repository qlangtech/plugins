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

package com.qlangtech.tis.plugin.ontology.impl.action;

import com.alibaba.citrus.turbine.Context;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
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
 * Action Type Step 2: 参数配置
 *
 * 定义 Action 的输入参数（全局参数池）
 * Parameters 是 ActionType 级别的全局变量，可以被多个 Rules 引用
 *
 * JSON 格式示例：
 * <pre>
 * [
 *   {
 *     "parameterId": "ticket",
 *     "displayName": "工单",
 *     "dataType": "OBJECT_REFERENCE",
 *     "objectTypeId": "Ticket",
 *     "required": true,
 *     "widgetType": "OBJECT_SELECTOR"
 *   },
 *   {
 *     "parameterId": "newPriority",
 *     "displayName": "新优先级",
 *     "dataType": "STRING",
 *     "required": true,
 *     "widgetType": "DROPDOWN",
 *     "validation": {
 *       "enum": ["P0", "P1", "P2"]
 *     }
 *   }
 * ]
 * </pre>
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/6
 */
public class ActionParameters extends OneStepOfMultiSteps {

    @FormField(ordinal = 0, type = FormFieldType.TEXTAREA, validate = {Validator.require})
    public String parameters;

    @Override
    public void processPreSaved(IPluginContext pluginContext, Context currentCtx,
                               OneStepOfMultiSteps[] preSavedStepPlugins) {
        // 保存当前步骤数据到上下文
        currentCtx.put(ActionParameters.class.getName(), this);
        super.processPreSaved(pluginContext, currentCtx, preSavedStepPlugins);
    }

    @TISExtension
    public static class Desc extends OneStepOfMultiSteps.BasicDesc {

        @Override
        public String getStepDescription() {
            return "Parameters";
        }

        @Override
        public Step getStep() {
            return Step.Step2;
        }

        @Override
        public Optional<BasicDesc> nextPluginDesc(OneStepOfMultiSteps current) {
            return Optional.of(new ActionRules.Desc());
        }

        @Override
        public boolean isFinalStep() {
            return false;
        }

        /**
         * 验证 parameters 字段
         * 确保是有效的 JSON 数组格式
         */
        public boolean validateParameters(IFieldErrorHandler msgHandler, Context context,
                                         String fieldName, String value) {
            if (StringUtils.isEmpty(value)) {
                msgHandler.addFieldError(context, fieldName, "参数配置不能为空");
                return false;
            }

            // 验证 JSON 格式
            try {
                JSONArray array = JSON.parseArray(value);
                if (array == null || array.isEmpty()) {
                    msgHandler.addFieldError(context, fieldName, "参数配置必须是非空的 JSON 数组");
                    return false;
                }

                // 验证每个参数对象的必需字段
                for (int i = 0; i < array.size(); i++) {
                    com.alibaba.fastjson.JSONObject param = array.getJSONObject(i);

                    if (!param.containsKey("parameterId") || StringUtils.isEmpty(param.getString("parameterId"))) {
                        msgHandler.addFieldError(context, fieldName,
                            "参数 #" + (i + 1) + " 缺少必需字段 'parameterId'");
                        return false;
                    }

                    if (!param.containsKey("displayName") || StringUtils.isEmpty(param.getString("displayName"))) {
                        msgHandler.addFieldError(context, fieldName,
                            "参数 #" + (i + 1) + " 缺少必需字段 'displayName'");
                        return false;
                    }

                    if (!param.containsKey("dataType") || StringUtils.isEmpty(param.getString("dataType"))) {
                        msgHandler.addFieldError(context, fieldName,
                            "参数 #" + (i + 1) + " 缺少必需字段 'dataType'");
                        return false;
                    }

                    // 验证 dataType 的有效性
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
                    "参数配置 JSON 格式错误: " + e.getMessage());
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
                dataType.equals("OBJECT_REFERENCE") ||
                dataType.equals("ARRAY") ||
                dataType.equals("STRUCT")
            );
        }
    }
}
