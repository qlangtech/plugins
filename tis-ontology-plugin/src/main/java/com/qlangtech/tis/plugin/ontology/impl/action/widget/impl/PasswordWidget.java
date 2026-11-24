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
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.ontology.impl.action.ActionParameterConfig;
import com.qlangtech.tis.plugin.ontology.impl.action.ValidateScriptBuffer;
import com.qlangtech.tis.plugin.ontology.impl.action.widget.UserInputWidgetType;

/**
 * 密码输入控件。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/15
 */
public class PasswordWidget extends UserInputWidgetType {

    @FormField(ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {})
    public String placeholder;

    @FormField(ordinal = 1, type = FormFieldType.INT_NUMBER, validate = {})
    public Integer minLength;

    @FormField(ordinal = 2, type = FormFieldType.INT_NUMBER, validate = {})
    public Integer maxLength;

    @Override
    public FormFieldType getFormFieldType() {
        return FormFieldType.PASSWORD;
    }

    @Override
    public Object getDefaultValue() {
        return null;
    }

    /**
     * 生成长度校验：提交值长度必须落在 {@link #minLength}~{@link #maxLength} 内，
     * 越界时 {@code addFieldError} 并返回 false。上下限各自判空，只配一端就只生成一端。
     */
    @Override
    public void generateParamVaildateScript(ValidateScriptBuffer pluginGroovySource, ActionParameterConfig paramConfig) {
        if (this.minLength == null && this.maxLength == null) {
            // 长度上下限都没配置，无需生成校验方法
            return;
        }
        pluginGroovySource.validateMethod(paramConfig.requireFieldName(), (script) -> {
            if (this.minLength != null) {
                script.addFieldErrorIf("value.length() < " + this.minLength, "长度不能小于" + this.minLength);
            }
            if (this.maxLength != null) {
                script.addFieldErrorIf("value.length() > " + this.maxLength, "长度不能大于" + this.maxLength);
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
            return FormFieldType.PASSWORD;
        }

        @Override
        public String getDisplayName() {
            return "Password";
        }
    }
}
