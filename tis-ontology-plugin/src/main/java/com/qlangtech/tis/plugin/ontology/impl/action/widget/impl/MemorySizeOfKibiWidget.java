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
import com.qlangtech.tis.plugin.ontology.impl.action.ActionParameterConfig;
import com.qlangtech.tis.plugin.ontology.impl.action.ValidateScriptBuffer;
import com.qlangtech.tis.plugin.ontology.impl.action.widget.UserInputWidgetType;

/**
 * 内存大小控件（KiB）。用户填的是 KiB 数，落盘时由
 * {@link FormFieldType#MEMORY_SIZE_OF_KIBI} 的输入处理器换算成 {@link MemorySize}。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/15
 */
public class MemorySizeOfKibiWidget extends UserInputWidgetType {

    @FormField(ordinal = 0, type = FormFieldType.MEMORY_SIZE_OF_KIBI, validate = {})
    public MemorySize defaultValue;

    @FormField(ordinal = 1, type = FormFieldType.MEMORY_SIZE_OF_KIBI, validate = {})
    public MemorySize min;

    @FormField(ordinal = 2, type = FormFieldType.MEMORY_SIZE_OF_KIBI, validate = {})
    public MemorySize max;

    @Override
    public FormFieldType getFormFieldType() {
        return FormFieldType.MEMORY_SIZE_OF_KIBI;
    }

    @Override
    public Object getDefaultValue() {
        return defaultValue == null ? null : defaultValue.getKibiBytes();
    }

    /**
     * 生成内存大小校验：提交值是纯数字串（单位不在值里，见 {@code FormFieldType} 的
     * MemorySizeValProcessor），按本控件的 KiB 单位比对 {@link #min}~{@link #max}。
     *
     * <p>MEMORY_SIZE_OF_KIBI 字段类型没有自带追加校验器（不同于 INT_NUMBER），格式守卫
     * 是唯一一道拦截，非法串若放行会在落盘绑定 MemorySize 时才炸。
     */
    @Override
    public void generateParamVaildateScript(ValidateScriptBuffer pluginGroovySource, ActionParameterConfig paramConfig) {
        if (this.min == null && this.max == null) {
            // 上下界都没配置，无需生成校验方法
            return;
        }
        pluginGroovySource.validateMethod(paramConfig.requireFieldName(), (script) -> {
            script.addFieldErrorIf("!value.isLong()", "必须是整型数字");
            script.startLine("long v = value.toLong()");
            if (this.min != null) {
                script.addFieldErrorIf("v < " + this.min.getKibiBytes(), "不能小于" + this.min.getKibiBytes() + "KiB");
            }
            if (this.max != null) {
                script.addFieldErrorIf("v > " + this.max.getKibiBytes(), "不能大于" + this.max.getKibiBytes() + "KiB");
            }
            script.startLine("return true");
        });
    }

    @Override
    protected void appendExtraPropsJson(JSONObject fieldJson) {
    }

    @TISExtension
    public static class DftDescriptor extends BasicDescriptor {

        @Override
        public FormFieldType getFormFieldType() {
            return FormFieldType.MEMORY_SIZE_OF_KIBI;
        }

        @Override
        public String getDisplayName() {
            return "Memory_Size_Of_Kibi";
        }
    }
}
