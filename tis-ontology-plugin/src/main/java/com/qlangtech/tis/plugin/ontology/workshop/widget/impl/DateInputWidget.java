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
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.workshop.widget.CompactDisplayWidget;
import com.qlangtech.tis.plugin.workshop.widget.IWorkshopWidget;

/**
 * P1 Widget：日期输入（Date Input）—— <b>范围</b>选择。
 * <p>
 * 对应官方文档 <i>Date Input</i>（见 Filtering widgets 总览：「Allow the user to enter a
 * single date or date range」）。本 Widget 只承载其中的<b>范围</b>那一半，单值归
 * {@link DateTimePickerWidget}，两者职责互斥、无功能重叠。
 *
 * <h3>为什么输出两个变量</h3>
 * {@code VariableType} 12 个常量里<b>没有任何区间 / range 类型</b>，一个范围在类型层面
 * 只能落到两个变量上（{@code startVar} + {@code endVar}）。这也与官方模型一致：
 * object set filter 的「Filter value extraction」在提取 Date/Time ranges 时，同样要求
 * 值经由 Date / Timestamp 这类基本变量传递。
 *
 * <h3>刻意没有的字段</h3>
 * <ul>
 *   <li>{@code pickerMode}（粒度）—— {@code nz-range-picker} 对 {@code nzMode} 的支持
 *       与单值 picker 不等价，加了容易出现「字段在但行为不符预期」。</li>
 *   <li>{@code placeholder} —— range picker 的占位是 {@code [start, end]} 二元组，
 *       单字符串字段语义不成立；展示文案由基类 {@code title} 承担。</li>
 * </ul>
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/20
 */
public class DateInputWidget extends CompactDisplayWidget {

    public static final String KEY_START_VAR = "startVar";
    public static final String KEY_END_VAR = "endVar";

    /** 是否显示时分秒 */
    @FormField(type = FormFieldType.ENUM, ordinal = 10, advance = false)
    public Boolean showTime;

    /** 输出变量格式：ISO 日期串 / 时间戳(毫秒) */
    @FormField(type = FormFieldType.ENUM, ordinal = 11, advance = false)
    public VariableKind variableKind;

    /**
     * 范围起始值绑定：用户选定区间后写入该变量。
     * <p>
     * options 同时接受 DATE 与 TIMESTAMP：{@link #variableKind} 只决定<b>写出的值</b>的
     * 格式，不决定目标变量应有的类型，故两者都列出由使用者自行选择。
     */
    @FormField(type = FormFieldType.SELECTABLE, ordinal = 12, advance = false, validate = {Validator.require})
    public String startVar;

    /** 范围结束值绑定：与 {@link #startVar} 成对写入 */
    @FormField(type = FormFieldType.SELECTABLE, ordinal = 13, advance = false, validate = {Validator.require})
    public String endVar;

    /**
     * 变量输出格式枚举。
     * <p>
     * ⚠️ 常量名必须与 {@link DateTimePickerWidget.VariableKind} 逐字一致（{@code iso} /
     * {@code timestamp}）—— 两个 Widget 写出的值会被同一批下游消费方解析，格式语义一旦
     * 分叉，下游会在<b>没有报错</b>的情况下解析错位。
     */
    public enum VariableKind {
        iso("ISO 日期串"), timestamp("时间戳(毫秒)");
        public final String label;
        VariableKind(String label) { this.label = label; }
    }

    @TISExtension
    public static class DescriptorImpl extends BaseWidgetDescriptor<IWorkshopWidget> {

        public DescriptorImpl() {
            super();
            this.registerSelectOptions(KEY_START_VAR, WidgetOptionHelper::getDateTimeVariableOptions);
            this.registerSelectOptions(KEY_END_VAR, WidgetOptionHelper::getDateTimeVariableOptions);
        }

        @Override
        public String getDisplayName() {
            return "Date Input";
        }

        @Override
        public WorkShopWidgetType getWidgetType() {
            return WorkShopWidgetType.DATE_INPUT;
        }
    }
}
