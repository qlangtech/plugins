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
import com.alibaba.fastjson.JSONObject;
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
import java.util.stream.Collectors;

/**
 * Action Type Step 3: 规则配置
 *
 * 定义 Action 的执行规则，包括规则类型、配置和提交条件
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/6
 */
public class ActionRules extends OneStepOfMultiSteps {

    @FormField(ordinal = 0, type = FormFieldType.ENUM, validate = {Validator.require})
    public String ruleType;

    @FormField(ordinal = 1, type = FormFieldType.TEXTAREA, validate = {Validator.require})
    public String ruleConfig;

    @FormField(ordinal = 2, type = FormFieldType.TEXTAREA, validate = {})
    public String submissionCriteria;

    @Override
    public void processPreSaved(IPluginContext pluginContext, Context currentCtx,
                               OneStepOfMultiSteps[] preSavedStepPlugins) {
        // 保存当前步骤数据到上下文
        currentCtx.put(ActionRules.class.getName(), this);
        super.processPreSaved(pluginContext, currentCtx, preSavedStepPlugins);
    }

    @TISExtension
    public static class Desc extends OneStepOfMultiSteps.BasicDesc {

        @Override
        public String getStepDescription() {
            return "Rules";
        }

        @Override
        public Step getStep() {
            return Step.Step3;
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
         * 获取 ruleType 的可选项
         */
        public List<Option> getRuleTypeOptions() {
            return Arrays.asList(
                new Option("CREATE_OBJECT", "创建对象"),
                new Option("MODIFY_OBJECT", "修改对象"),
                new Option("CREATE_OR_MODIFY_OBJECT", "创建或修改对象"),
                new Option("DELETE_OBJECT", "删除对象"),
                new Option("CREATE_LINK", "创建链接"),
                new Option("DELETE_LINK", "删除链接"),
                new Option("FUNCTION_BACKED", "函数支持"),
                new Option("NOTIFICATION", "发送通知"),
                new Option("WEBHOOK", "调用外部 API"),
                new Option("WRITEBACK_WEBHOOK", "编辑前调用外部 API"),
                new Option("SCHEDULE", "触发数据构建"),
                new Option("APPLY_SCENARIO", "合并场景编辑")
            );
        }

        /**
         * 验证 ruleType 字段
         */
        public boolean validateRuleType(IFieldErrorHandler msgHandler, Context context,
                                       String fieldName, String value) {
            if (StringUtils.isEmpty(value)) {
                msgHandler.addFieldError(context, fieldName, "规则类型不能为空");
                return false;
            }

            List<String> validTypes = getRuleTypeOptions().stream()
                .map(Option::getName)
                .collect(Collectors.toList());

            if (!validTypes.contains(value)) {
                msgHandler.addFieldError(context, fieldName,
                    "无效的规则类型: " + value);
                return false;
            }

            return true;
        }

        /**
         * 验证 ruleConfig 字段
         */
        public boolean validateRuleConfig(IFieldErrorHandler msgHandler, Context context,
                                         String fieldName, String value) {
            if (StringUtils.isEmpty(value)) {
                msgHandler.addFieldError(context, fieldName, "规则配置不能为空");
                return false;
            }

            // 验证 JSON 格式
            try {
                JSONObject config = JSON.parseObject(value);
                if (config == null || config.isEmpty()) {
                    msgHandler.addFieldError(context, fieldName,
                        "规则配置必须是非空的 JSON 对象");
                    return false;
                }

                // 根据规则类型验证必需字段
                // 这里可以根据不同的 ruleType 进行更详细的验证
                // 暂时只验证基本的 JSON 格式

                return true;

            } catch (Exception e) {
                msgHandler.addFieldError(context, fieldName,
                    "规则配置 JSON 格式错误: " + e.getMessage());
                return false;
            }
        }

        /**
         * 验证 submissionCriteria 字段（可选）
         */
        public boolean validateSubmissionCriteria(IFieldErrorHandler msgHandler, Context context,
                                                 String fieldName, String value) {
            // submissionCriteria 是可选的
            if (StringUtils.isEmpty(value)) {
                return true;
            }

            // 如果提供了值，验证 JSON 格式
            try {
                JSONObject criteria = JSON.parseObject(value);
                if (criteria == null) {
                    msgHandler.addFieldError(context, fieldName,
                        "提交条件必须是有效的 JSON 对象");
                    return false;
                }

                // 验证基本结构（可选）
                if (criteria.containsKey("propertyConditions")) {
                    if (!(criteria.get("propertyConditions") instanceof com.alibaba.fastjson.JSONArray)) {
                        msgHandler.addFieldError(context, fieldName,
                            "propertyConditions 必须是 JSON 数组");
                        return false;
                    }
                }

                return true;

            } catch (Exception e) {
                msgHandler.addFieldError(context, fieldName,
                    "提交条件 JSON 格式错误: " + e.getMessage());
                return false;
            }
        }
    }
}
