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
 * P1 Widget：标题文本（Header Text）
 * <p>
 * 渲染标题文字，支持 4 级标题、对齐方式和预设颜色选择。
 * level、alignment、color 均使用 ENUM 枚举，前端可选值固定。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/9
 */
public class HeaderTextWidget extends WorkshopWidget {

    public static final String KEY_TEXT_VARIABLE = "textVariable";

    /** 标题文本内容 */
    @FormField(type = FormFieldType.INPUTTEXT, ordinal = 10, advance = false, validate = {Validator.require})
    public String text;

    /** 文本变量绑定（字符串变量，可选） */
    @FormField(type = FormFieldType.SELECTABLE, ordinal = 11, advance = false, validate = {Validator.require})
    public String textVariable;

    /** 图标（ng-zorro 图标名，如 font-size） */
    @FormField(type = FormFieldType.INPUTTEXT, ordinal = 12, advance = false, validate = {Validator.require})
    public String icon;

    /** 标题级别 1-4 */
    @FormField(type = FormFieldType.ENUM, ordinal = 13, advance = false)
    public HeaderLevel level;

    /** 文本对齐方式 */
    @FormField(type = FormFieldType.ENUM, ordinal = 14, advance = false)
    public TextAlignment alignment;

    /** 文本颜色（20 种预设色，hex 色值在 .json resource 中定义） */
    @FormField(type = FormFieldType.ENUM, ordinal = 15, advance = false)
    public WidgetColor color;

    /** 标题级别枚举 */
    public enum HeaderLevel {
        H1("1"), H2("2"), H3("3"), H4("4");
        public final String label;
        HeaderLevel(String label) { this.label = label; }
    }

    /** 文本对齐方式枚举 */
    public enum TextAlignment {
        left("左对齐"), center("居中"), right("右对齐");
        public final String label;
        TextAlignment(String label) { this.label = label; }
    }

    /** 20 种常用颜色枚举，hex 色值在 .json resource 中定义，前端可渲染颜色预览圆点 */
    public enum WidgetColor {
        inherit("继承"), primary("品牌色"), red("红色"), volcano("火山橙"),
        orange("橙色"), gold("金色"), yellow("黄色"), lime("黄绿"),
        green("绿色"), cyan("青色"), blue("蓝色"), geekblue("靛蓝"),
        purple("紫色"), magenta("品红"), grey("灰色"), darkGrey("深灰"),
        black("黑色"), white("白色"), success("成功绿"), error("错误红");
        public final String label;
        WidgetColor(String label) { this.label = label; }
    }

    @TISExtension
    public static class DescriptorImpl extends BaseWidgetDescriptor<IWorkshopWidget> {

        public DescriptorImpl() {
            super();
            this.registerSelectOptions(KEY_TEXT_VARIABLE, WidgetOptionHelper::getStringVariableOptions);
        }

        @Override
        public String getDisplayName() {
            return "Header Text";
        }

        @Override
        public WorkShopWidgetType getWidgetType() {
            return WorkShopWidgetType.HEADER_TEXT;
        }
    }
}