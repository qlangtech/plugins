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

package com.qlangtech.tis.plugin.ontology.impl.action;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.impl.action.widget.UserInputWidgetType;

/**
 * 基本数据类型（primitive types）参数配置。
 *
 * <p>用于 Action 参数值为标量类型（STRING/INTEGER/DATE 等）的场景，
 * 包含类型选择、是否必填、以及前端控件类型配置。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/13
 * @see ActionParameterDataType
 */
public class PrimitiveDataType extends ActionParameterDataType {



    /**
     * 参数在 Action 表单上用什么控件收集（供后续前端渲染使用）。
     *
     * <p>控件种类本身是多态的：每个 {@link UserInputWidgetType} 子类对应一种控件，
     * 并自带该控件专属的配置（文本框的占位提示与校验规则、数字的上下界、下拉的候选选项……）。
     * 因此这里声明成聚合属性字段，让用户在界面上先选控件种类、再填该种类的专属参数，
     * 而不是把各控件的字段平铺到本类上、再靠 {@code validateAll()} 里的 if 兜底。
     */
    @FormField(ordinal = 1, validate = {Validator.require})
    public UserInputWidgetType widgetType;
    /**
     * 是否必填。
     */
    @FormField(ordinal = 2, type = FormFieldType.ENUM, validate = {Validator.require})
    public Boolean required = true;
    /**
     * 输入属性的帮助说明信息，用于在最终表单里给该参数显示引导文案。
     */
    @FormField(ordinal = 3, type = FormFieldType.INPUTTEXT)
    public String help;


    @Override
    public String getTypeIdentifier() {
        return "primitive:" + (widgetType == null ? "unset" : widgetType.getFormFieldType().name());
    }

    @TISExtension
    public static class DftDescriptor extends BasicDescriptor implements DescriptorUseableShortComment {

        @Override
        public String getDisplayName() {
            return "Primitive_Type";
        }

        @Override
        public String shortComment() {
            return "基本类型";
        }
    }
}