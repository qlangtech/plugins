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
package com.qlangtech.tis.plugin.ontology.workshop.model.event;

import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;

import java.util.List;

/** 跳转到另一个 Workshop 模块（前端 {@code open-workshop-module}）。 */
public class OpenWorkshopModuleConfig extends EventConfig {

    private static final long serialVersionUID = 1L;

    /** 目标模块的标识。当前没有可供列举全部模块的选项来源，故为文本输入。 */
    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String moduleId;

    @FormField(ordinal = 2, type = FormFieldType.MULTI_DESCRIBLE_PLUGIN, desClazz = VariableMappingConfig.class)
    public List<VariableMappingConfig> variableMapping;

    @TISExtension
    public static class DefaultDescriptor extends BasicDescriptor {
        @Override
        public String getDisplayName() {
            return "Open_Workshop_Module";
        }

        @Override
        public String shortComment() {
            return "打开 Workshop 模块";
        }
    }
}
