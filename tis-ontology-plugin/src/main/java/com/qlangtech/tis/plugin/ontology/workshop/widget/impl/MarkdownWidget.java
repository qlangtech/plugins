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
import com.qlangtech.tis.plugin.ontology.workshop.widget.WorkshopWidget;
import com.qlangtech.tis.plugin.workshop.widget.IWorkshopWidget;

/**
 * P1 Widget：Markdown 渲染
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/9
 */
public class MarkdownWidget extends WorkshopWidget {

    /** Markdown 文本内容（支持 {{varName}} 模板变量替换） */
    @FormField(type = FormFieldType.TEXTAREA, ordinal = 13, advance = false, validate = {Validator.require})
    public String content;

    public static final String KEY_CONTENT_VAR = "contentVar";

    /**
     * 内容来源变量：可选。与 {@link #content} 是两种写法 —— 前者取当前模块中某个字符串变量的值
     * 作为渲染内容，后者是构建者手写的静态文本。两者互不排斥，由使用者择一。
     */
    @FormField(type = FormFieldType.SELECTABLE, ordinal = 14, advance = false)
    public String contentVar;

    @TISExtension
    public static class DescriptorImpl extends BaseWidgetDescriptor<IWorkshopWidget> {

        public DescriptorImpl() {
            super();
            this.registerSelectOptions(KEY_CONTENT_VAR, WidgetOptionHelper::getStringVariableOptions);
        }

        @Override
        public String getDisplayName() {
            return "Markdown";
        }

        @Override
        public WorkShopWidgetType getWidgetType() {
            return WorkShopWidgetType.MARKDOWN;
        }
    }
}