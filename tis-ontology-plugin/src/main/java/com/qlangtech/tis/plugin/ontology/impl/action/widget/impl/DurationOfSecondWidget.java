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

import java.time.Duration;

/**
 * 时长控件（秒）。用户填的是秒数，落盘时由 {@link FormFieldType#DURATION_OF_SECOND}
 * 的输入处理器换算成 {@link Duration}，与 {@link DurationOfMinuteWidget} /
 * {@link DurationOfHourWidget} 三者不可混用——单位写错会静默换算成另一个量级。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/15
 */
public class DurationOfSecondWidget extends UserInputWidgetType {

    @FormField(ordinal = 0, type = FormFieldType.DURATION_OF_SECOND, validate = {})
    public Duration defaultValue;

    @FormField(ordinal = 1, type = FormFieldType.DURATION_OF_SECOND, validate = {})
    public Duration min;

    @FormField(ordinal = 2, type = FormFieldType.DURATION_OF_SECOND, validate = {})
    public Duration max;

    @Override
    public FormFieldType getFormFieldType() {
        return FormFieldType.DURATION_OF_SECOND;
    }

    @Override
    public Object getDefaultValue() {
        return defaultValue == null ? null : defaultValue.toSeconds();
    }

    @Override
    protected void appendExtraPropsJson(JSONObject fieldJson) {
    }

    @TISExtension
    public static class DftDescriptor extends BasicDescriptor {

        @Override
        public FormFieldType getFormFieldType() {
            return FormFieldType.DURATION_OF_SECOND;
        }

        @Override
        public String getDisplayName() {
            return "Duration_Of_Second";
        }
    }
}
