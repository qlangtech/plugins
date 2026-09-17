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
package com.qlangtech.tis.plugin.ontology.workshop.widget.impl;

import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.ontology.workshop.widget.WorkshopWidget;
import com.qlangtech.tis.plugin.workshop.widget.IWorkshopWidget;

/**
 * P1 Widget：日期时间选择器（Date Time Picker）
 * <p>
 * 支持日期选择/范围选择模式、日/周/月/年粒度切换，
 * 以及 ISO 日期串/时间戳两种输出变量格式。
 * mode、pickerMode、variableKind 均使用 ENUM 枚举。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/9
 */
public class DateTimePickerWidget extends WorkshopWidget {

    /** 是否显示时分秒 */
    @FormField(type = FormFieldType.ENUM, ordinal = 13, advance = false)
    public Boolean showTime;

    /** 日期选择模式：日期选择 / 范围选择 */
    @FormField(type = FormFieldType.ENUM, ordinal = 14, advance = false)
    public DatePickerMode mode;

    /** 选择器粒度：日 / 周 / 月 / 年 */
    @FormField(type = FormFieldType.ENUM, ordinal = 15, advance = false)
    public PickerMode pickerMode;

    /** 变量输出格式：ISO 日期串 / 时间戳(毫秒) */
    @FormField(type = FormFieldType.ENUM, ordinal = 16, advance = true)
    public VariableKind variableKind;

    /** 占位提示文本 */
    @FormField(type = FormFieldType.INPUTTEXT, ordinal = 17, advance = false)
    public String placeholder;

    public static final String KEY_DATE_VALUE_VAR = "dateValueVar";

    /**
     * 选中日期绑定：<b>双向</b> —— 组件既监听该变量取值作为初始值，也在用户选择后向其写回。
     * options 同时接受 DATE 与 TIMESTAMP：{@link #variableKind} 只决定<b>写出的值</b>的格式，
     * 不决定目标变量应有的类型。
     */
    @FormField(type = FormFieldType.SELECTABLE, ordinal = 18, advance = false)
    public String dateValueVar;

    /** 日期选择模式枚举 */
    public enum DatePickerMode {
        date("日期选择"), range("范围选择");
        public final String label;
        DatePickerMode(String label) { this.label = label; }
    }

    /** 选择器粒度枚举 */
    public enum PickerMode {
        date("日"), week("周"), month("月"), year("年");
        public final String label;
        PickerMode(String label) { this.label = label; }
    }

    /** 变量输出格式枚举 */
    public enum VariableKind {
        iso("ISO 日期串"), timestamp("时间戳(毫秒)");
        public final String label;
        VariableKind(String label) { this.label = label; }
    }

    @TISExtension
    public static class DescriptorImpl extends BaseWidgetDescriptor<IWorkshopWidget> {

        public DescriptorImpl() {
            super();
            this.registerSelectOptions(KEY_DATE_VALUE_VAR, WidgetOptionHelper::getDateTimeVariableOptions);
        }

        @Override
        public String getDisplayName() {
            return "Date Time Picker";
        }

        @Override
        public WorkShopWidgetType getWidgetType() {
            return WorkShopWidgetType.DATE_TIME_PICKER;
        }
    }
}