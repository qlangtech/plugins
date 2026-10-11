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

package com.qlangtech.tis.plugin.ontology.impl.action;

import com.google.common.collect.Lists;
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.IPluginStore;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.OntologyType;

import java.util.List;

/**
 * Action 参数池中的一行参数定义，供 {@code MULTI_DESCRIBLE_PLUGIN} 子表单渲染。
 *
 * <p>替代原先 {@link ActionParameters#parameters} 里让用户手写 JSON 数组字符串的做法
 * （违反工程 CLAUDE.md 原则一）。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/11
 * @see ActionParameters
 */
public class ActionParameterConfig implements Describable<ActionParameterConfig>,
        IPluginStore.MultiDescribleElement {

    /**
     * 参数唯一标识。规则里的映射行以该名字引用参数，同时它也是本行的记录键。
     *
     * <p>刻意不加 {@code Validator.identity}：该校验器只允许 {@code [A-Z\da-z_\-]+}，
     * 会拒绝存量里可能出现的其它写法，而 identity 的唯一性要求由「一行一个键」的表格语义保证。
     */
    @FormField(identity = true, ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String parameterId;

    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String displayName;

    /**
     * 数据类型。
     *
     * <p>这里用 {@code int} + json 里的 {@code OntologyType.availableTypes()} 候选脚本，
     * 而不是直接声明 {@link OntologyType} 类型的字段——后者在本体工程里的既有写法都是 int：
     * {@code MetadataOfValueType.type}、{@code DefaultPropertyTypeRef.type} 两个先例都是
     * {@code int} 字段 + {@code OntologyType.parse(value)}。
     * 原因是 {@link OntologyType} 上挂了自定义的 {@code OntologyTypeJSONSerializer}，
     * 直接把枚举当表单字段值序列化会写出一个对象而不是标量。
     *
     * @see OntologyType
     */
    @FormField(ordinal = 2, type = FormFieldType.ENUM, validate = {Validator.require})
    public int dataType;

    /**
     * 目标对象类型，仅 {@link OntologyType#OBJECT_REFERENCE} 时有意义。
     */
    @FormField(ordinal = 3, type = FormFieldType.INPUTTEXT, validate = {})
    public String objectTypeId;

    @FormField(ordinal = 4, type = FormFieldType.ENUM, validate = {})
    public Boolean required = true;

    @FormField(ordinal = 5, type = FormFieldType.ENUM, validate = {})
    public WidgetType widgetType = WidgetType.TEXT_INPUT;

    /**
     * 取本行声明的本体类型，0（未选）会抛出可执行的错误。
     */
    public OntologyType parseDataType() {
        return OntologyType.parse(this.dataType);
    }

    @Override
    public String identityValue() {
        return this.parameterId;
    }

    /**
     * 参数在 Action 表单上用什么控件收集（供后续前端渲染使用）
     */
    public enum WidgetType {
        TEXT_INPUT, DROPDOWN, OBJECT_SELECTOR
    }

    @TISExtension
    public static class DftDescriptor extends IPluginStore.MultiDescribleElementDescriptor<ActionParameterConfig> {

        @Override
        public String getDisplayName() {
            return "Parameter";
        }

        @Override
        public String shortComment() {
            return "Action 的输入参数";
        }

        @Override
        public ViewStyle viewStyle() {
            return ViewStyle.Table;
        }

        @Override
        public List<ColConfig> colsConfig() {
            // Table 视图要求至少有一列被标记为 clickable，否则 getExtractProps 抛异常
            // （IPluginStore.MultiDescribleElementDescriptor#getExtractProps）。
            // 列出的键必须都是本类上真实存在的 @FormField。
            // objectTypeId / widgetType 不列：前者只在 OBJECT_REFERENCE 时有意义，后者是渲染细节。
            return Lists.newArrayList(new ColConfig("parameterId").setClickable(), new ColConfig("displayName"),
                    new ColConfig("dataType"), new ColConfig("required", 12));
        }
    }
}
