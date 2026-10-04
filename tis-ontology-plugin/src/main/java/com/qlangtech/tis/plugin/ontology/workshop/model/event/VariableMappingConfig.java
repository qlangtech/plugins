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

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.IPluginStore;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.WidgetOptionHelper;

import java.io.Serializable;

/**
 * 跨模块跳转时的变量映射：把当前模块的变量值带到目标模块的变量上。
 * 对应前端 {@code VariableMapping} 接口，字段名与之一致。
 */
public class VariableMappingConfig implements Describable<VariableMappingConfig>, Serializable,
        IPluginStore.MultiDescribleElement {

    private static final long serialVersionUID = 1L;

    public static final String KEY_SOURCE_VARIABLE_ID = "sourceVariableId";
    public static final String KEY_TARGET_VARIABLE_ID = "targetVariableId";

    @FormField(identity = true, ordinal = 0, type = FormFieldType.SELECTABLE, validate = {Validator.require})
    public String sourceVariableId;

    @FormField(ordinal = 1, type = FormFieldType.SELECTABLE, validate = {Validator.require})
    public String targetVariableId;

    @Override
    public String identityValue() {
        return this.sourceVariableId;
    }

    @TISExtension
    public static class DftDescriptor extends Descriptor<VariableMappingConfig> {
        public DftDescriptor() {
            super();
            this.registerSelectOptions(KEY_SOURCE_VARIABLE_ID, WidgetOptionHelper::getWorkshopVariableOptions);
            this.registerSelectOptions(KEY_TARGET_VARIABLE_ID, WidgetOptionHelper::getWorkshopVariableOptions);
        }

        @Override
        public String getDisplayName() {
            return "变量映射";
        }
    }
}
