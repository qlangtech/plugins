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
import com.qlangtech.tis.extension.OneStepOfMultiSteps;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.manage.common.Option;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.Ontology;
import com.qlangtech.tis.plugin.ontology.OntologyObjectType;
import com.qlangtech.tis.plugin.ontology.impl.OntologyPluginMeta;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import com.qlangtech.tis.util.IPluginContext;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Action Type Step 1: 元数据配置
 *
 * 定义 Action 的基本信息：名称、显示名、描述、目标对象类型
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/6
 */
public class ActionMetadata extends OneStepOfMultiSteps {

    @FormField(ordinal = 0, validate = {Validator.require, Validator.identity})
    public String name;

    @FormField(ordinal = 1, validate = {Validator.require})
    public String displayName;

    @FormField(ordinal = 2, type = FormFieldType.TEXTAREA, validate = {Validator.require})
    public String description;

    @FormField(ordinal = 3, type = FormFieldType.ENUM, validate = {Validator.require})
    public String targetObjectType;

    @Override
    public void processPreSaved(IPluginContext pluginContext, Context currentCtx,
                               OneStepOfMultiSteps[] preSavedStepPlugins) {
        // 保存当前步骤数据到上下文，供后续步骤使用
        currentCtx.put(ActionMetadata.class.getName(), this);
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
            return Optional.of(new ActionParameters.Desc());
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
                msgHandler.addFieldError(context, fieldName, "Action 名称不能为空");
                return false;
            }

            // 验证命名规范：只允许字母、数字、下划线、连字符
            if (!value.matches("^[a-zA-Z][a-zA-Z0-9_-]*$")) {
                msgHandler.addFieldError(context, fieldName,
                    "Action 名称必须以字母开头，只能包含字母、数字、下划线和连字符");
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
         * 获取 targetObjectType 的可选项
         * 从当前本体域加载所有 ObjectType
         */
        public List<Option> getTargetObjectTypeOptions() {
            try {
                // 从上下文获取本体域名称
                com.qlangtech.tis.plugin.ontology.impl.OntologyPluginMeta meta =
                    com.qlangtech.tis.plugin.ontology.impl.OntologyPluginMeta.createPluginMeta(
                        com.qlangtech.tis.util.UploadPluginMeta.parse(Ontology.KEY_ONTOLOGY)
                    );
                if (meta == null) {
                    return List.of();
                }

                String ontologyDomain = meta.getDomain();
                if (StringUtils.isEmpty(ontologyDomain)) {
                    return List.of();
                }

                // 加载该域下的所有 ObjectType
                List<OntologyObjectType> objectTypes =
                    Ontology.OntologyEnum.ObjectType.loadAll(
                        com.qlangtech.tis.plugin.ontology.impl.OntologyPluginMeta.create(
                            Ontology.OntologyEnum.ObjectType, ontologyDomain)
                    );

                return objectTypes.stream()
                    .map(objType -> new Option(objType.identityValue(), objType.getName()))
                    .collect(Collectors.toList());

            } catch (Exception e) {
                // 如果加载失败，返回空列表
                return List.of();
            }
        }

        /**
         * 验证 targetObjectType 字段
         */
        public boolean validateTargetObjectType(IFieldErrorHandler msgHandler, Context context,
                                               String fieldName, String value) {
            if (StringUtils.isEmpty(value)) {
                msgHandler.addFieldError(context, fieldName, "目标对象类型不能为空");
                return false;
            }
            return true;
        }
    }
}
