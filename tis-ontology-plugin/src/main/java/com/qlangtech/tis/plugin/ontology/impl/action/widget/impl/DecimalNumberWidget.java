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
 * 小数输入控件。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/15
 */
public class DecimalNumberWidget extends UserInputWidgetType {

    @FormField(ordinal = 0, type = FormFieldType.DECIMAL_NUMBER, validate = {})
    public Double defaultValue;

    @FormField(ordinal = 1, type = FormFieldType.DECIMAL_NUMBER, validate = {})
    public Double min;

    @FormField(ordinal = 2, type = FormFieldType.DECIMAL_NUMBER, validate = {})
    public Double max;

    /**
     * 小数位数；留空表示不限制。
     */
    @FormField(ordinal = 3, type = FormFieldType.INT_NUMBER, validate = {})
    public Integer scale;

    @Override
    public FormFieldType getFormFieldType() {
        return FormFieldType.DECIMAL_NUMBER;
    }

    @Override
    public Object getDefaultValue() {
        return defaultValue;
    }

    /**
     * 生成数字校验：①格式守卫 ②小数位数上限 ③min/max 范围，各自判空按需生成。
     *
     * <p>格式守卫是唯一一道拦截：INT_NUMBER 自带追加的 {@code Validator.integer}
     * （FormFieldType#getAppendValueValidator），DECIMAL_NUMBER 没有，非法串若放行
     * 会在落盘绑定 Double 时才炸。
     */
    @Override
    public void generateParamVaildateScript(ValidateScriptBuffer pluginGroovySource, ActionParameterConfig paramConfig) {
        if (this.min == null && this.max == null && this.scale == null) {
            // 上下界和小数位都没配置，无需生成校验方法
            return;
        }
        pluginGroovySource.validateMethod(paramConfig.requireFieldName(), (script) -> {
            script.addFieldErrorIf("!value.isBigDecimal()", "必须是数字");
            script.startLine("BigDecimal v = value.toBigDecimal()");
            if (this.scale != null) {
                // stripTrailingZeros：'1.10' 的有效小数位按 1 计而非 2
                script.addFieldErrorIf("v.stripTrailingZeros().scale() > " + this.scale
                        , "小数位数不能超过" + this.scale);
            }
            if (this.min != null) {
                script.addFieldErrorIf("v < " + this.min, "不能小于" + this.min);
            }
            if (this.max != null) {
                script.addFieldErrorIf("v > " + this.max, "不能大于" + this.max);
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
            return FormFieldType.DECIMAL_NUMBER;
        }

        @Override
        public String getDisplayName() {
            return "Decimal_Number";
        }
    }
}
