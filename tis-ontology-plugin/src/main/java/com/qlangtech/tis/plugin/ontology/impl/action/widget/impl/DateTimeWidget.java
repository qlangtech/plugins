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
import com.qlangtech.tis.datax.TimeFormat;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.util.PluginExtraProps;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.ontology.impl.action.ActionParameterConfig;
import com.qlangtech.tis.plugin.ontology.impl.action.ValidateScriptBuffer;
import com.qlangtech.tis.plugin.ontology.impl.action.widget.UserInputWidgetType;

import java.util.Date;

/**
 * 日期时间控件。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/15
 */
public class DateTimeWidget extends UserInputWidgetType {

    /**
     * {@link FormFieldType#DateTime} 的输入处理器只接受 {@code Long} 或 {@link java.util.Date}
     * 作为目标类型（见 {@code FormFieldType.DateTime} 的 IPropValProcessor），这里取后者。
     */
    @FormField(ordinal = 0, type = FormFieldType.DateTime, validate = {})
    public Date defaultValue;

    @FormField(ordinal = 1, type = FormFieldType.ENUM, validate = {})
    public TimeFormat displayFormat;


    @Override
    public FormFieldType getFormFieldType() {
        return FormFieldType.DateTime;
    }

    @Override
    public String getDefaultValue() {
        if ((defaultValue) == null) {
            return null;
        }
        //  "dateTimeFormat": "yyyy-MM-dd HH:mm:ss"
        return getDisplayFormat().format(defaultValue);
    }

    /**
     * 生成日期时间格式校验：提交值必须严格符合 {@link #displayFormat} 的 pattern
     * （未配置回落 {@code yyyy-MM-dd HH:mm:ss}）。
     *
     * <p>与运行时提交契约对齐：{@code appendExtraPropsJson} 会把 pattern 写进 extra-props 的
     * {@code dateTimeFormat} key，前端 DateTime 组件按它格式化提交值，
     * {@code FormFieldType.DateTime} 的输入处理器也按它解析。
     */
    @Override
    public void generateParamVaildateScript(ValidateScriptBuffer pluginGroovySource, ActionParameterConfig paramConfig) {
        String pattern = this.getDisplayFormat().pattern;
        pluginGroovySource.validateMethod(paramConfig.requireFieldName(), (script) -> {
            script.rejectInvalidDate(pattern, "日期时间不符合格式" + pattern);
            script.startLine("return true");
        });
    }

    @Override
    protected void appendExtraPropsJson(JSONObject fieldJson) {
        fieldJson.put(PluginExtraProps.KEY_DATETIME_FORMAT, getDisplayFormat().pattern);
    }

    public static void main(String[] args) {
        System.out.println(TimeFormat.yyyy_MM_dd.timeFormatter.toString());
    }

    public TimeFormat getDisplayFormat() {
        return displayFormat == null ? TimeFormat.yyyyMMdd_HH_mm_ss : displayFormat;
    }


    @TISExtension
    public static class DftDescriptor extends BasicDescriptor {

        @Override
        public FormFieldType getFormFieldType() {
            return FormFieldType.DateTime;
        }

        @Override
        public String getDisplayName() {
            return "Date_Time";
        }
    }
}
