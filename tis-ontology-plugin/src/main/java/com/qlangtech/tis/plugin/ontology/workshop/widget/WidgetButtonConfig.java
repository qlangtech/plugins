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
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.workshop.model.ConditionalVisibility;

import java.io.Serializable;

/**
 * {@link com.qlangtech.tis.plugin.ontology.workshop.widget.impl.ButtonGroupWidget} 里的一颗按钮。
 *
 * <p>字段集合与前端 {@code button-group.component.ts} 的模板逐字对应
 * （{@code btn.type} / {@code btn.danger} / {@code btn.size} / {@code btn.leftIcon} /
 * {@code btn.rightIcon} / {@code btn.text} / {@code btn.conditionalVisibility} /
 * {@code btn.onClick}），前端无需做字段重命名。
 *
 * <p>与 {@link WidgetColumnConfig} / {@link WidgetActionConfig} 同属
 * {@code MultiDescribleElement} 一族 —— 由 {@code MULTI_DESCRIBLE_PLUGIN} + {@code desClazz}
 * 驱动子表单渲染，无需自定义行编辑器。
 *
 * <h3>枚举常量名的尾下划线</h3>
 * {@code default} 是 Java 关键字，无法作枚举常量名，故写作 {@link ButtonType#default_} /
 * {@link ButtonSize#default_}；前端渲染到 Ant Design 的 {@code nzType} / {@code nzSize} 前
 * 统一经 {@code antdToken()} 去掉尾下划线。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/19
 */
public class WidgetButtonConfig implements Describable<WidgetButtonConfig>, Serializable, IPluginStore.MultiDescribleElement {

    private static final long serialVersionUID = 1L;

    /** 按钮文案，同时作为子表单行的唯一标识 */
    @FormField(identity = true, ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String text;

    /** 按钮形态 */
    @FormField(ordinal = 1, type = FormFieldType.ENUM)
    public ButtonType type = ButtonType.default_;

    /** 危险按钮（红色），用于删除/重置这类不可逆操作 */
    @FormField(ordinal = 2, type = FormFieldType.ENUM)
    public Boolean danger = false;

    /** 按钮尺寸 */
    @FormField(ordinal = 3, type = FormFieldType.ENUM)
    public ButtonSize size = ButtonSize.default_;

    /** 图标（Ant Design 图标名），文案左侧 */
    @FormField(ordinal = 4, type = FormFieldType.INPUTTEXT, advance = true)
    public String leftIcon;

    /** 图标（Ant Design 图标名），文案右侧 */
    @FormField(ordinal = 5, type = FormFieldType.INPUTTEXT, advance = true)
    public String rightIcon;

    /** 条件可见性：随某个变量真假改变按钮的呈现（不配则恒显示） */
    @FormField(ordinal = 6, advance = false)
    public ConditionalVisibility conditionalVisibility;

    /** 点击行为（四型多态，见 {@link WidgetClickAction}） */
    @FormField(ordinal = 7, advance = false)
    public WidgetClickAction onClick;

    @Override
    public String identityValue() {
        return this.text;
    }

    /** 按钮形态；取值固定，硬编码于此（CLAUDE.md 原则二：固定取值用 ENUM） */
    public enum ButtonType implements DescriptorUseableShortComment {
        primary("主按钮：页面主操作"),
        default_("次按钮：默认样式"),
        dashed("虚线按钮"),
        text("文字按钮：无边框背景"),
        link("链接按钮：呈现为文字链接");

        public final String label;

        ButtonType(String label) {
            this.label = label;
        }

        @Override
        public String shortComment() {
            return this.label;
        }
    }

    /** 按钮尺寸；取值固定，硬编码于此 */
    public enum ButtonSize implements DescriptorUseableShortComment {
        large("大"), default_("中"), small("小");

        public final String label;

        ButtonSize(String label) {
            this.label = label;
        }

        @Override
        public String shortComment() {
            return this.label;
        }
    }

    @TISExtension
    public static class DftDescriptor extends Descriptor<WidgetButtonConfig> {
        @Override
        public String getDisplayName() {
            return "Button";
        }
    }
}
