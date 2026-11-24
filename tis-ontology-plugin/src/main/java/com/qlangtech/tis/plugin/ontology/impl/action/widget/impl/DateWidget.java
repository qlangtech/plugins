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
 * 日期选择控件（不含时分秒）。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/15
 */
public class DateWidget extends UserInputWidgetType {

    /**
     * {@link FormFieldType#DATE} 的字段类型是 String，格式为 {@code yyyy-MM-dd}。
     */
    @FormField(ordinal = 0, type = FormFieldType.DATE, validate = {})
    public String defaultValue;

    /**
     * 前端展示格式，如 {@code yyyy-MM-dd}；留空走控件默认值。
     */
    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {})
    public String displayFormat;

    @Override
    public FormFieldType getFormFieldType() {
        return FormFieldType.DATE;
    }

    @Override
    public Object getDefaultValue() {
        return defaultValue;
    }

    /**
     * 生成日期格式校验：提交值必须是严格 {@code yyyy-MM-dd}（lenient 关闭且整串消费，
     * 拒绝「2026-01-01xxx」这类尾部夹带垃圾的输入）。
     *
     * <p>刻意不用 {@link #displayFormat} 做校验 pattern——它没有写入 extra-props
     * （{@code appendExtraPropsJson} 为空），是纯展示配置；运行时前端 DATE 组件固定按
     * {@code yyyy-MM-dd} 提交，拿它校验会误杀合法值。
     */
    @Override
    public void generateParamVaildateScript(ValidateScriptBuffer pluginGroovySource, ActionParameterConfig paramConfig) {
        pluginGroovySource.validateMethod(paramConfig.requireFieldName(), (script) -> {
            script.rejectInvalidDate("yyyy-MM-dd", "日期不符合格式yyyy-MM-dd");
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
            return FormFieldType.DATE;
        }

        @Override
        public String getDisplayName() {
            return "Date";
        }
    }
}
