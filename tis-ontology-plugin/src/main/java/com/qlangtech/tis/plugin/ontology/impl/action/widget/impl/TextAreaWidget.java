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
import com.qlangtech.tis.plugin.annotation.TextAreaContentMode;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.impl.action.ActionParameterConfig;
import com.qlangtech.tis.plugin.ontology.impl.action.ValidateScriptBuffer;
import com.qlangtech.tis.plugin.ontology.impl.action.widget.UserInputWidgetType;
import org.apache.commons.lang3.StringUtils;

import java.util.Objects;

import static com.qlangtech.tis.extension.util.PluginExtraProps.KEY_PLACEHOLDER_PROP;

/**
 * 多行文本控件。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/15
 */
public class TextAreaWidget extends UserInputWidgetType {


    @FormField(ordinal = 0, type = FormFieldType.TEXTAREA, validate = {})
    public String defaultValue;

    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {})
    public String placeholder;

    /**
     * 文本域可见行数；留空由前端按默认高度渲染。
     */
    @FormField(ordinal = 2, type = FormFieldType.INT_NUMBER, validate = {Validator.require, Validator.integer})
    public Integer rows;

    @FormField(ordinal = 3, type = FormFieldType.ENUM, validate = {Validator.require})
    public TextAreaContentMode contentMode;

    @Override
    public FormFieldType getFormFieldType() {
        return FormFieldType.TEXTAREA;
    }

    @Override
    public Object getDefaultValue() {
        return defaultValue;
    }

    /**
     * 无运行时取值约束：{@code rows}/{@code contentMode} 是展示形态配置，不构成取值合法性
     * 边界（内容是否为合法 JSON/XML 等属于业务层校验，不在生成期铺开），故不生成校验方法。
     */
    @Override
    public void generateParamVaildateScript(ValidateScriptBuffer pluginGroovySource, ActionParameterConfig paramConfig) {
    }

    @Override
    protected void appendExtraPropsJson(JSONObject fieldJson) {
        if (StringUtils.isNotEmpty(placeholder)) {
            fieldJson.put(KEY_PLACEHOLDER_PROP, this.placeholder);
        }
        fieldJson.put("rows", rows != null ? rows : 3);
        fieldJson.put("mode", Objects.requireNonNull(contentMode, "contentMode can not be null").codeMirrorToken);
        fieldJson.put("style", "codemirror");
    }

    @TISExtension
    public static class DftDescriptor extends BasicDescriptor {

        public DftDescriptor() {
            super();
            //this.registerSelectOptions(KEY_VALIDATORS, UserInputWidgetType::validatorOptions);
        }

        @Override
        public FormFieldType getFormFieldType() {
            return FormFieldType.TEXTAREA;
        }

        @Override
        public String getDisplayName() {
            return "Text_Area";
        }
    }
}
