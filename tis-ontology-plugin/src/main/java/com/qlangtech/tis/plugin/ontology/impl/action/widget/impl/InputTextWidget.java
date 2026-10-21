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
import com.qlangtech.tis.plugin.ontology.impl.action.widget.UserInputWidgetType;
import org.apache.commons.lang3.StringUtils;

import static com.qlangtech.tis.extension.util.PluginExtraProps.KEY_PLACEHOLDER_PROP;

/**
 * 单行文本控件。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/15
 */
public class InputTextWidget extends UserInputWidgetType {

    //public static final String KEY_VALIDATORS = "validators";

    @FormField(ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {})
    public String defaultValue;

    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {})
    public String placeholder;

//    @FormField(ordinal = 2, type = FormFieldType.ENUM, validate = {})
//    public List<Validator> validators = Lists.newArrayList();

    @Override
    public FormFieldType getFormFieldType() {
        return FormFieldType.INPUTTEXT;
    }

    @Override
    public Object getDefaultValue() {
        return defaultValue;
    }

    @Override
    protected void appendExtraPropsJson(JSONObject fieldJson) {

        if (StringUtils.isNotEmpty(placeholder)) {
            fieldJson.put(KEY_PLACEHOLDER_PROP, this.placeholder);
        }
    }

    @TISExtension
    public static class DftDescriptor extends BasicDescriptor {

        public DftDescriptor() {
            super();
            // MULTI_SELECTABLE 字段必须在 Descriptor 构造期注册选项供给方，
            // 否则前端渲染该字段时抛 "is select options has not been register"
            //this.registerSelectOptions(KEY_VALIDATORS, UserInputWidgetType::validatorOptions);
        }

        @Override
        public FormFieldType getFormFieldType() {
            return FormFieldType.INPUTTEXT;
        }

        @Override
        public String getDisplayName() {
            return "Input_Text";
        }
    }
}
