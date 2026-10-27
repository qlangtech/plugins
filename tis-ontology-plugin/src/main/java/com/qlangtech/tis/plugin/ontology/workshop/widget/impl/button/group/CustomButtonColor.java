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
package com.qlangtech.tis.plugin.ontology.workshop.widget.impl.button.group;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.workshop.widget.ButtonColor;
import com.qlangtech.tis.plugin.ontology.workshop.util.HexColorValidator;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;

/**
 * 自定义：十六进制色值。唯一带 {@code customColor} 载荷的子类 —— 见 {@link ButtonColor}
 * 的多态理由。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/20
 */
public class CustomButtonColor extends ButtonColor {

    private static final long serialVersionUID = 1L;



    /**
     * 自定义色值（十六进制，如 #f5222d）
     */
    @FormField(ordinal = 0,type = FormFieldType.INPUTTEXT,validate = {Validator.require})
    public String customColor;

    @TISExtension
    public static class DefaultDescriptor extends BasicDescriptor implements DescriptorUseableShortComment {
        @Override
        public String getDisplayName() {
            return "custom";
        }

        /**
         * {@code customColor} 的格式校验 —— 框架按 {@code validate${字段名}} 约定反射发现
         * （见 {@code Descriptor#createValidateMap}），空值不进来（由 {@link Validator#require} 负责）。
         */
        public boolean validateCustomColor(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            return HexColorValidator.validate(msgHandler, context, fieldName, value);
        }

        @Override
        public String shortComment() {
            return "自定义：十六进制色值";
        }
    }
}
