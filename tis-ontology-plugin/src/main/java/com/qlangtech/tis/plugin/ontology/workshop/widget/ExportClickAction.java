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
package com.qlangtech.tis.plugin.ontology.workshop.widget;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.WidgetOptionHelper;

/**
 * 点击后把某个对象集变量的当前值导出为文件。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/19
 */
public class ExportClickAction extends WidgetClickAction {

    private static final long serialVersionUID = 1L;

    public static final String KEY_OBJECT_SET_VAR = "objectSetVar";

    /** 要导出的对象集变量 */
    @FormField(type = FormFieldType.SELECTABLE, ordinal = 0, validate = {Validator.require})
    public String objectSetVar;

    /** 导出文件格式 */
    @FormField(ordinal = 1, type = FormFieldType.ENUM)
    public ExportFormat format = ExportFormat.csv;

    /** 导出文件名（不含扩展名）；留空时前端按对象集变量名 + 时间戳生成 */
    @FormField(ordinal = 2, type = FormFieldType.INPUTTEXT, advance = true)
    public String fileName;

    /** 导出格式；取值固定，硬编码于此（CLAUDE.md 原则二） */
    public enum ExportFormat implements DescriptorUseableShortComment {
        csv("CSV：逗号分隔的纯文本表格"),
        xlsx("Excel：带表头的 xlsx 工作簿");

        public final String label;

        ExportFormat(String label) {
            this.label = label;
        }

        @Override
        public String shortComment() {
            return this.label;
        }
    }

    @TISExtension
    public static class DefaultDescriptor extends BasicDescriptor {

        public DefaultDescriptor() {
            super();
            this.registerSelectOptions(KEY_OBJECT_SET_VAR, WidgetOptionHelper::getObjectSetVariableOptions);
        }

        @Override
        public String shortComment() {
            return "导出数据";
        }

        @Override
        public String getDisplayName() {
            return "Export";
        }
    }
}
