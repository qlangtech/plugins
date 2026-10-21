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

package com.qlangtech.tis.plugin.ontology.impl.action.widget.impl;

import com.alibaba.fastjson.JSONObject;

import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.MemorySize;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.ontology.impl.action.widget.UserInputWidgetType;

/**
 * 内存大小控件（字节）。用户填的是字节数，落盘时由
 * {@link FormFieldType#MEMORY_SIZE_OF_BYTE} 的输入处理器换算成 {@link MemorySize}。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/15
 */
public class MemorySizeOfByteWidget extends UserInputWidgetType {

    @FormField(ordinal = 0, type = FormFieldType.MEMORY_SIZE_OF_BYTE, validate = {})
    public MemorySize defaultValue;

    @FormField(ordinal = 1, type = FormFieldType.MEMORY_SIZE_OF_BYTE, validate = {})
    public MemorySize min;

    @FormField(ordinal = 2, type = FormFieldType.MEMORY_SIZE_OF_BYTE, validate = {})
    public MemorySize max;

    @Override
    public FormFieldType getFormFieldType() {
        return FormFieldType.MEMORY_SIZE_OF_BYTE;
    }

    @Override
    public Object getDefaultValue() {
        return defaultValue == null ? null : defaultValue.getBytes();
    }

    @Override
    protected void appendExtraPropsJson(JSONObject fieldJson) {
    }

    @TISExtension
    public static class DftDescriptor extends BasicDescriptor {

        @Override
        public FormFieldType getFormFieldType() {
            return FormFieldType.MEMORY_SIZE_OF_BYTE;
        }

        @Override
        public String getDisplayName() {
            return "Memory_Size_Of_Byte";
        }
    }
}
