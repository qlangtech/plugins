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
import com.qlangtech.tis.plugin.ontology.workshop.widget.CompactDisplayWidget;
import com.qlangtech.tis.plugin.ontology.workshop.widget.WorkshopWidget;
import com.qlangtech.tis.plugin.workshop.widget.IWorkshopWidget;

/**
 * P1 Widget：日期时间选择器（Date Time Picker）
 * <p>
 * 对应官方文档 <i>Date and Time Picker</i> —— <b>单值</b>日期时间输入。
 * 支持日/周/月/年粒度切换，以及 ISO 日期串/时间戳两种输出变量格式。
 * pickerMode、variableKind 均使用 ENUM 枚举。
 * <p>
 * 原先的 {@code DatePickerMode mode}（date / range）已删除：范围选择在官方文档中归属另一个
 * Widget —— <i>Date Input</i>（见 Filtering widgets 总览「enter a single date or date range」），
 * 本 Widget 只负责单值。更重要的是该字段属静默失效：前端虽按 {@code mode} 渲染了
 * {@code nz-range-picker}，但 {@code onRangeChange} 是空实现，选中范围不会写回任何变量，
 * 且 {@code dateValueVar} 是单变量字段，结构上也承载不了 start/end 两个值。
 * 若后续需要范围选择，应另开 DateInputWidget 承载。{@code ordinal = 14} 由此留空
 * （见 {@link WorkshopWidget} 字段序位约定：序位留空无害，不必重排）。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/9
 */
public class DateTimePickerWidget extends CompactDisplayWidget {

    /** 是否显示时分秒 */
    @FormField(type = FormFieldType.ENUM, ordinal = 13, advance = false)
    public Boolean showTime;

    /** 选择器粒度：日 / 周 / 月 / 年 */
    @FormField(type = FormFieldType.ENUM, ordinal = 15, advance = false)
    public PickerMode pickerMode;

    /** 变量输出格式：ISO 日期串 / 时间戳(毫秒) */
    @FormField(type = FormFieldType.ENUM, ordinal = 16, advance = false)
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