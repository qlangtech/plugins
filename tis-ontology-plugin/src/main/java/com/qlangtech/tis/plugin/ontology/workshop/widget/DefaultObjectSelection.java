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
 * 支持多选：每个对象前出现勾选框，勾选集合写回指定对象集变量。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/11/10
 */
public class DefaultObjectSelection extends ObjectSelection {

    private static final long serialVersionUID = 1L;

    public static final String KEY_SELECTED_OBJECTS_VAR = "selectedObjectsVar";

    /**
     * 选中集合写回的对象集变量。必填 —— 开了多选却没绑变量，勾选结果无处可去。
     */
    @FormField(ordinal = 0, type = FormFieldType.SELECTABLE, validate = {Validator.require})
    public String selectedObjectsVar;

    @Override
    public boolean multiSelectEnabled() {
        return true;
    }

    @Override
    public String resolveSelectedObjectsVar() {
        return selectedObjectsVar;
    }

    /**
     * 无参构造是必需的：{@code clazz = getClass().getEnclosingClass()} 靠它把 descriptor 绑到
     * {@code DefaultObjectSelection} 上，写成带参（或移到基类里）两个子类的 {@code getId()} 会撞号。
     */
    @TISExtension
    public static class DefaultDescriptor extends BasicDescriptor implements DescriptorUseableShortComment {

        public DefaultDescriptor() {
            super();
            this.registerSelectOptions(KEY_SELECTED_OBJECTS_VAR, WidgetOptionHelper::getObjectSetVariableOptions);
        }

        @Override
        public String getDisplayName() {
            return "Default";
        }

        @Override
        public String shortComment() {
            return "支持多选，勾选集合写回指定变量";
        }
    }
}
