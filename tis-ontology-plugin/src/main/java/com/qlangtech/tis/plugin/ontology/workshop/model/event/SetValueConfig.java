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
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.WidgetOptionHelper;

/** 把一个变量的值赋给另一个变量（前端 {@code set-value}）。 */
public class SetValueConfig extends EventConfig {

    private static final long serialVersionUID = 1L;

    public static final String KEY_SOURCE_VARIABLE_ID = "sourceVariableId";
    public static final String KEY_TARGET_VARIABLE_ID = "targetVariableId";

    @FormField(type = FormFieldType.SELECTABLE, ordinal = 1, validate = {Validator.require})
    public String sourceVariableId;

    @FormField(type = FormFieldType.SELECTABLE, ordinal = 2, validate = {Validator.require})
    public String targetVariableId;

    @TISExtension
    public static class DefaultDescriptor extends BasicDescriptor {

        public DefaultDescriptor() {
            super();
            this.registerSelectOptions(KEY_SOURCE_VARIABLE_ID, WidgetOptionHelper::getWorkshopVariableOptions);
            this.registerSelectOptions(KEY_TARGET_VARIABLE_ID, WidgetOptionHelper::getWorkshopVariableOptions);
        }

        @Override
        public String getDisplayName() {
            return "Set_Value";
        }

        @Override
        public String shortComment() {
            return "赋值变量";
        }
    }
}
