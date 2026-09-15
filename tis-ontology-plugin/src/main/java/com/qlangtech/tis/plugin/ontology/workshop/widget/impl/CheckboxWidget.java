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
import com.qlangtech.tis.plugin.workshop.widget.IWorkshopWidget;
import com.qlangtech.tis.plugin.workshop.widget.WorkshopWidget;

/**
 * P1 Widget：布尔开关（Checkbox）
 * <p>
 * 渲染单个 nz-checkbox，可绑定布尔型输出变量。
 * 非常适合用于条件过滤或开关控制场景。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/9
 */
public class CheckboxWidget extends WorkshopWidget {
    /**
     * 复选框的标签文本
     */
    @FormField(type = FormFieldType.INPUTTEXT, ordinal = 13, advance = false, validate = {Validator.none_blank})
    public String label;

    public static final String KEY_CHECKED_VAR = "checkedVar";

    /**
     * 勾选状态绑定：<b>双向</b> —— 组件既监听该变量取值作为初始勾选态，也在用户勾选后向其写回。
     */
    @FormField(type = FormFieldType.SELECTABLE, ordinal = 14, advance = false)
    public String checkedVar;

    @TISExtension
    public static class DescriptorImpl extends BaseWidgetDescriptor<IWorkshopWidget> {

        public DescriptorImpl() {
            super();
            this.registerSelectOptions(KEY_CHECKED_VAR, WidgetOptionHelper::getBooleanVariableOptions);
        }

        @Override
        public String getDisplayName() {
            return "Checkbox";
        }

        @Override
        public WorkShopWidgetType getWidgetType() {
            return WorkShopWidgetType.CHECKBOX;
        }
    }
}