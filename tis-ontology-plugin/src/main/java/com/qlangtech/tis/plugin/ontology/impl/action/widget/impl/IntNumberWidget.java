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

import com.alibaba.citrus.turbine.Context;
import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.impl.action.ActionParameterConfig;
import com.qlangtech.tis.plugin.ontology.impl.action.ValidateScriptBuffer;
import com.qlangtech.tis.plugin.ontology.impl.action.widget.UserInputWidgetType;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;

/**
 * 整数输入控件。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/15
 */
public class IntNumberWidget extends UserInputWidgetType {

    @FormField(ordinal = 0, type = FormFieldType.INT_NUMBER, validate = {})
    public Integer defaultValue;

    @FormField(ordinal = 1, type = FormFieldType.INT_NUMBER, validate = {Validator.integer})
    public Integer min;

    @FormField(ordinal = 2, type = FormFieldType.INT_NUMBER, validate = {Validator.integer})
    public Integer max;

    @Override
    public FormFieldType getFormFieldType() {
        return FormFieldType.INT_NUMBER;
    }

    @Override
    public Object getDefaultValue() {
        return defaultValue;
    }

    /**
     * 为本参数生成 groovy 校验方法 {@code validate<FieldId>}：输入值必须落在
     * {@link #min}~{@link #max} 区间内，越界时 {@code addFieldError} 并返回 false。
     *
     * <p>方法签名与 addFieldError 调用形态统一由 {@link ValidateScriptBuffer} 的
     * {@code validateMethod}/{@code addFieldErrorIf} 组装——签名（4 参 + primitive boolean
     * 返回）是 {@code Descriptor#createValidateMap} 反射注册的硬约束，首参必须声明为
     * {@code IFieldErrorHandler}（写成子类 IControlMsgHandler 会导致方法<b>静默不注册</b>）；
     * 第 4 参 value 是原始 {@code _primaryVal} 提交串。min/max 在生成期烘焙成字面量，
     * 生成的 Desc 无法反查 widget 实例。
     */
    @Override
    public void generateParamVaildateScript(ValidateScriptBuffer pluginGroovySource, ActionParameterConfig paramConfig) {
        if (this.min == null && this.max == null) {
            // 上下界都没配置，无需生成校验方法
            return;
        }
        pluginGroovySource.validateMethod(paramConfig.requireFieldName(), (script) -> {
            // 正常链路上 INT_NUMBER 字段类型自带追加的 Validator.integer 会先拦掉非法格式
            // （FormFieldType#getAppendValueValidator），这里的 isLong 守卫是第二道保险：
            // validateXxx 被直接 invoke 时避免 NumberFormatException 打爆整个校验请求；
            // 用 isLong 而非 isInteger：超大数字串 isInteger 会漏判导致校验被静默跳过
            script.methodBody("if (!value.isLong())", (c) -> {
                c.startLine("return true");
            });
            script.startLine("long v = value.toLong()");
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
            return FormFieldType.INT_NUMBER;
        }

        @Override
        protected boolean validateAll(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {

            IntNumberWidget intInput = postFormVals.newInstance();
//            if (intInput.min == null || intInput.max == null) {
//                return true;
//            }
            if (intInput.min != null && intInput.max != null && intInput.min > intInput.max) {
                msgHandler.addFieldError(context, "min", "不能大于Max输入值");
                return false;
            }
            return true;
        }

        @Override
        public String getDisplayName() {
            return "Int_Number";
        }
    }
}
