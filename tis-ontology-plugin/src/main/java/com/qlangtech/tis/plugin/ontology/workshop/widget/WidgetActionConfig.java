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

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.IPluginStore;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.WidgetOptionHelper;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;

import java.io.Serializable;

/**
 * 卡片/列表行内嵌的操作按钮配置。
 *
 * <p>用于 {@code ObjectListWidget.cardActions} 这类「每一行/每一张卡片上挂一个操作按钮」的场景：
 * 按钮点击后触发一个本体动作（Ontology Action），或把当前行对象写回某个变量。
 *
 * <p>与 {@link WidgetColumnConfig} 同属 {@code MultiDescribleElement} 一族 ——
 * 由 {@code FormFieldType.MULTI_DESCRIBLE_PLUGIN} + {@code desClazz} 驱动子表单渲染，
 * 无需自定义行编辑器。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/19
 */
public class WidgetActionConfig implements Describable<WidgetActionConfig>, Serializable, IPluginStore.MultiDescribleElement {

    private static final long serialVersionUID = 1L;

    /**
     * 按钮文案，同时作为子表单行的唯一标识
     */
    @FormField(identity = true, ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String text;

    /**
     * 按钮形态
     */
    @FormField(ordinal = 1, type = FormFieldType.ENUM, validate = {Validator.require})
    public ButtonType buttonType = ButtonType.primary;

    /** 字段名常量：供 Descriptor 注册选项时引用 */
    public static final String KEY_ACTION_ID = "actionId";

    /**
     * 触发的本体动作标识（当前本体域中已定义的 Action 名）。
     *
     * <p>选项由 {@link WidgetOptionHelper#getOntologyActionOptions()} 按域供给 ——
     * 见 {@code DftDescriptor} 构造器里的 {@code registerSelectOptions}。
     */
    @FormField(ordinal = 2, type = FormFieldType.SELECTABLE, validate = {Validator.require})
    public String actionId;

    /**
     * 图标（Ant Design 图标名）
     */
    @FormField(ordinal = 3, type = FormFieldType.INPUTTEXT)
    public String icon;

    /**
     * 执行前是否二次确认
     */
    @FormField(ordinal = 4, type = FormFieldType.ENUM)
    public Boolean confirmBeforeExecute = false;

    @Override
    public String identityValue() {
        return this.text;
    }

    /**
     * 按钮形态；取值固定，硬编码于此（CLAUDE.md 原则二：固定取值用 ENUM）
     */
    public enum ButtonType implements DescriptorUseableShortComment {
        primary("主按钮"), default_("次按钮"), dashed("虚线"), text("文字"), link("链接");

        private final String comment;

        ButtonType(String comment) {
            this.comment = comment;
        }

        @Override
        public String shortComment() {
            return comment;
        }
    }

    @TISExtension
    public static class DftDescriptor extends Descriptor<WidgetActionConfig> {

        public DftDescriptor() {
            super();
            this.registerSelectOptions(KEY_ACTION_ID, WidgetOptionHelper::getOntologyActionOptions);
        }

        @Override
        public String getDisplayName() {
            return "Action";
        }
    }
}