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

package com.qlangtech.tis.plugin.ontology.impl.action.rule;

import com.alibaba.citrus.turbine.Context;
import com.google.common.collect.Lists;
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.IPluginStore;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * 一行「目标字段 ← 取值来源」的映射，供 {@code MULTI_DESCRIBLE_PLUGIN} 子表单渲染。
 *
 * <p>一个元素类服务三处：{@link CreateObjectRule} 与 {@link ModifyObjectRule} 的
 * {@code propertyMappings}（target 是对象属性名）、{@link FunctionBackedRule} 的
 * {@code parameterMappings}（target 是函数形参名）。
 *
 * <p>取代原先 {@code propertyName1=paramName1\npropertyName2=paramName2} 的扁平文本
 * （违反工程 CLAUDE.md 原则一）。结构化顺手修掉一个语义漏洞：旧格式里
 * {@code updatedAt=CURRENT_TIME} 会被当成「名为 CURRENT_TIME 的参数」去查，查不到就静默跳过，
 * 也就是说旧注释里写的这个示例<b>在旧格式下根本表达不出来</b>；现在它是
 * {@link ValueSource#CURRENT_TIME} 这个显式取值来源。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/11
 * @see OntologyActionRule#buildMappedValues(List, java.util.Map)
 */
public class PropertyMappingConfig implements Describable<PropertyMappingConfig>,
        IPluginStore.MultiDescribleElement {

    /**
     * 目标字段名。Create/ModifyObjectRule 时是对象属性名，FunctionBackedRule 时是函数形参名。
     * 同时是本行的记录键（identity）——前端以 identity 字段的值为 key，同值静默覆盖，
     * 所以「同一个目标字段映射两次」在这里表达不出来，本也就是矛盾配置。
     */
    @FormField(identity = true, ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String target;

    @FormField(ordinal = 1, type = FormFieldType.ENUM, validate = {Validator.require})
    public ValueSource source = ValueSource.FROM_PARAMETER;

    /**
     * 仅在 {@link ValueSource#FROM_PARAMETER} 时有效。
     *
     * <p>没有下拉候选：参数池在第 3 步声明，而规则在第 2 步配置，规则保存时参数池尚未存在。
     * 行间的「参数是否存在」校验因此做不了，由 {@link DftDescriptor#validateAll} 兜住「选了
     * FROM_PARAMETER 就必须填参数名」这一半。
     */
    @FormField(ordinal = 2, type = FormFieldType.INPUTTEXT, validate = {})
    public String parameterRef;

    /**
     * 仅在 {@link ValueSource#LITERAL} 时有效。
     */
    @FormField(ordinal = 3, type = FormFieldType.INPUTTEXT, validate = {})
    public String literalValue;

    @Override
    public String identityValue() {
        return this.target;
    }

    /**
     * 取值来源。三个常量取自被取代的 {@code ActionRules.json} 里自己写的目标形态
     * （{@code {"valueSource":{"type":"FROM_PARAMETER","parameterRef":"ticket"}}} /
     * {@code {"type":"CURRENT_TIME"}}）。
     */
    public enum ValueSource {
        /** 取自 Action 参数池中的某个参数 */
        FROM_PARAMETER,
        /** 取当前时间，无需参数 */
        CURRENT_TIME,
        /** 写死的字面量 */
        LITERAL
    }

    @TISExtension
    public static class DftDescriptor extends IPluginStore.MultiDescribleElementDescriptor<PropertyMappingConfig> {

        @Override
        public String getDisplayName() {
            return "Mapping";
        }

        @Override
        public String shortComment() {
            return "目标字段 ← 取值来源";
        }

        @Override
        public ViewStyle viewStyle() {
            return ViewStyle.Table;
        }

        @Override
        public List<ColConfig> colsConfig() {
            // Table 视图要求至少一列 clickable，否则 getExtractProps 抛异常。
            // literalValue 不列成列：只在 LITERAL 时才有值，列出来三列里两列常空。
            return Lists.newArrayList(
                    new ColConfig("target").setClickable(),
                    new ColConfig("source"),
                    new ColConfig("parameterRef"));
        }

        /**
         * 「选了 FROM_PARAMETER 就必须填参数名」是同一行内两个字段之间的约束，
         * 字段级的 {@code validateXxx} 表达不了（{@code @FormField} 的 validate 是静态的）。
         *
         * <p>与同工程的 {@code Range4Integer.DefaultDesc#validateAll}（min 必须小于 max）
         * 是同一手法——本插件把 {@code validateAll} 用作列表元素的行级校验入口。
         *
         * <p>注意这里只兜得住「填没填」。参数名是否真的存在于参数池<b>校验不了</b>：
         * 参数池在第 3 步声明，而规则在第 2 步配置，规则保存时参数池尚未存在。
         */
        @Override
        protected boolean validateAll(IControlMsgHandler msgHandler, Context context, PostFormVals vals) {
            PropertyMappingConfig m = vals.newInstance();
            if (m.source == ValueSource.FROM_PARAMETER && StringUtils.isEmpty(m.parameterRef)) {
                msgHandler.addErrorMessage(context,
                        "字段「" + m.target + "」的取值来源是「Action 参数」，请填写参数名");
                return false;
            }
            return super.validateAll(msgHandler, context, vals);
        }
    }
}
