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
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.impl.EnumFieldMode;
import com.qlangtech.tis.manage.common.Option;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.impl.action.widget.UserInputWidgetType;
import com.qlangtech.tis.plugin.ontology.impl.action.widget.WidgetOptionConfig;

import java.util.List;

import static com.qlangtech.tis.extension.Descriptor.KEY_ENUM_EXTERNAL_PROPERTY_ALLOW_CUSTOMIZE_INPUT;

/**
 * 枚举控件：候选项是一张有限的离散清单，由 {@link #options} 逐条给出，并可用 {@link #showStyle}
 * 决定渲染成单选下拉还是多选列表。
 *
 * <p>与 {@link SelectableWidget} 的区别只在「选项从哪来」这一层的使用约定上：本控件的选项通常来自
 * 本体属性所引用的值类型的枚举约束（{@code Enum4String} / {@code Enum4Integer} 的 {@code enumVals}），
 * 由 {@code ActionParameterMapper} 自动预填，用户一般不必手改；而 {@code SelectableWidget} 是纯手填。
 * 两者字段形状相同，都复用 {@link WidgetOptionConfig}。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/15
 */
public class EnumWidget extends UserInputWidgetType {


    @FormField(ordinal = 5, desClazz = WidgetOptionConfig.class, type = FormFieldType.MULTI_DESCRIBLE_PLUGIN, validate =
            {Validator.require})
    public List<WidgetOptionConfig> options;

    /**
     * 需要等于 {@link #options} 中某个候选值的 {@code val}；留空表示不预选。
     */
    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {})
    public String defaultValue;

    @FormField(ordinal = 2, type = FormFieldType.ENUM, validate = {Validator.require})
    public EnumFieldMode showStyle;

    /**
     * 是否允许用户输入「选项列表里没有」的值（默认关闭）。
     * 打开后：在搜索框里输入的内容会作为一条「新增」选项排在下拉最前面，
     * 点击或回车即选中，字段值就是用户输入的文本。
     * <p>
     * 仅对 nzMode='default' 生效；multiple/tags 模式本身不是单值字段，不做改造。
     */
    @FormField(ordinal = 3, type = FormFieldType.ENUM, advance = true, validate = {Validator.require})
    public Boolean allowCustomizeInput;


    @Override
    public Object getDefaultValue() {
        return defaultValue;
    }

    @Override
    public FormFieldType getFormFieldType() {
        return FormFieldType.ENUM;
    }

    @Override
    public void appendExtraPropsJson(JSONObject fieldJson) {
        if (options != null && !options.isEmpty()) {
            fieldJson.put(Descriptor.KEY_ENUM_PROP, Option.toJson(toOptions(options)));
        }
        //"enumMode": "multiple",
        fieldJson.put(EnumFieldMode.KEY_ENUM_MODE, showStyle.val);
        if (allowCustomizeInput != null && allowCustomizeInput) {
            fieldJson.put(KEY_ENUM_EXTERNAL_PROPERTY_ALLOW_CUSTOMIZE_INPUT, true);
        }
    }

    @TISExtension
    public static class DftDescriptor extends BasicDescriptor {

        @Override
        public FormFieldType getFormFieldType() {
            return FormFieldType.ENUM;
        }

        @Override
        public String getDisplayName() {
            return "Enum";
        }
    }
}
