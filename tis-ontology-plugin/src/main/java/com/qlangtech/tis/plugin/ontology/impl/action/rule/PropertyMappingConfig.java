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

import com.google.common.collect.Lists;
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.manage.common.Option;
import com.qlangtech.tis.plugin.IPluginStore;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.OntologyObjectType;
import com.qlangtech.tis.plugin.ontology.OntologyProperty;
import com.qlangtech.tis.plugin.ontology.OntologyType;
import com.qlangtech.tis.plugin.ontology.impl.action.type.ObjectActionType;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.Objects;

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
 * {@link CurrentTimeValueSource} 这个显式取值来源。
 *
 * <p>2026/10/12 重构：将原先的枚举 {@code ValueSource} + 条件字段（{@code parameterRef}、
 * {@code literalValue}）的耦合设计改为 {@link ValueSourceConfig} 抽象类 + 三个子类的多态设计，
 * 消除字段间的关联性耦合。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/11
 * @see OntologyActionRule#buildMappedValues(List, java.util.Map)
 */
@Deprecated
public class PropertyMappingConfig implements Describable<PropertyMappingConfig>,
        IPluginStore.MultiDescribleElement {

    /**
     * 目标字段名。Create/ModifyObjectRule 时是对象属性名，FunctionBackedRule 时是函数形参名。
     * 同时是本行的记录键（identity）——前端以 identity 字段的值为 key，同值静默覆盖，
     * 所以「同一个目标字段映射两次」在这里表达不出来，本也就是矛盾配置。
     */
    @FormField(identity = true, ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String target;

    /**
     * 取值来源配置。
     *
     * <p>多态字段，TIS 自动检测为嵌套子表单，前端渲染为下拉选择具体类型 + 对应表单。
     * 不指定 type，框架根据字段类型（Describable 子类）自动处理。
     *
     * <p>三个可选实现：
     * <ul>
     *   <li>{@link FromParameterValueSource} - 从 Action 参数池取值</li>
     *   <li>{@link CurrentTimeValueSource} - 取当前时间（无额外字段）</li>
     *   <li>{@link LiteralValueSource} - 固定字面量</li>
     * </ul>
     */
    @FormField(ordinal = 1, validate = {Validator.require})
    public ValueSourceConfig source;

    @Override
    public String identityValue() {
        return this.target;
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

//        @Override
//        protected List<String> toFieldRelevantKeysCreator(String toPropertyKey, Descriptor descriptor) {
//            if (descriptor instanceof ObjectActionType.DefaultDescriptor actionType) {
//                return Lists.newArrayList(ObjectActionType.KEY_OBJECT_TYPE_REFERENCE, ObjectActionType.KEY_OBJECT_RULE);
//            } else {
//                throw new UnsupportedOperationException("unsupport descriptor:" + descriptor.getClass().getName());
//            }
//        }

        @Override
        protected RelevantFieldKeys createRelevantFieldKeysCreator(String toPropertyKey) {
            return new RelevantFieldKeys(toPropertyKey) {
                private Descriptor descriptor;

                @Override
                protected List<String> toFieldValiateRelevantKeysCreator(Descriptor descriptor) {
                    this.descriptor = descriptor;
                    if (descriptor instanceof ObjectActionType.DefaultDescriptor actionType) {
                        return Lists.newArrayList(ObjectActionType.KEY_OBJECT_TYPE_REFERENCE);
                    } else {
                        throw new UnsupportedOperationException("unsupport descriptor:" + descriptor.getClass().getName());
                    }
                }

                @Override
                protected List<String> toFieldNewInstanceRelevantKeysCreator() {
                    if (Objects.requireNonNull(descriptor, "descriptor can not be null")
                            instanceof ObjectActionType.DefaultDescriptor actionType) {
                        return Lists.newArrayList(ObjectActionType.KEY_OBJECT_RULE);
                    } else {
                        throw new UnsupportedOperationException("unsupport descriptor:" + descriptor.getClass().getName());
                    }
                }
            };
        }

        @Override
        public EnumableSetCfg getEnumableSetCfg() {
            return new EnumableSetCfg(true
                    // 设置为1 意味着取父parent的parent
                    , 1);
        }

        @Override
        protected List<Pair<Option, IPluginStore.MultiDescribleElement>> getEnumableCandidateSet(ParseDescribable<?> describable) {
            List<Pair<Option, IPluginStore.MultiDescribleElement>> target = Lists.newArrayList();

            // 因为 EnumableSetCfg(true, 1) 设置了跳过 1 层，所以这里拿到的是 ObjectActionType
            ObjectActionType actionType = describable.getInstance();

            // 加载对象类型及其属性列表
            OntologyObjectType objectType = actionType.loadObjectType();
            List<OntologyProperty> cols = objectType.getCols();

            // 提取当前已选中的 target 集合
            OntologyActionRule rule = actionType.getRule();
            java.util.Set<String> selected = new java.util.HashSet<>();
            List<PropertyMappingConfig> existingMappings = null;

            if (rule instanceof CreateObjectRule) {
                existingMappings = ((CreateObjectRule) rule).propertyMappings;
            } else if (rule instanceof ModifyObjectRule) {
                existingMappings = ((ModifyObjectRule) rule).propertyMappings;
            }

            if (existingMappings != null) {
                selected = existingMappings.stream()
                        .map(PropertyMappingConfig::identityValue)
                        .collect(java.util.stream.Collectors.toSet());
            }
            OntologyType type = null;
            // 为每个属性创建候选项
            for (OntologyProperty col : cols) {
                PropertyMappingConfig config = new PropertyMappingConfig();
                config.target = col.getName();

                type = col.parseOntologyType();
                ValueSourceConfig param = null;
                switch (type) {
                    case DATE, TIMESTAMP -> {
                        param = new CurrentTimeValueSource();
                    }
                    default -> {
                        param = new FromParameterValueSource(col.getName());
                    }
                }
                config.source = param;
                target.add(Pair.of(
                        new Option(col.getName()).setChecked(selected.contains(col.getName())),
                        config
                ));
            }

            return target;
        }


        @Override
        public ViewStyle viewStyle() {
            return ViewStyle.Table;
        }

        @Override
        public List<ColConfig> colsConfig() {
            // Table 视图要求至少一列 clickable，否则 getExtractProps 抛异常。
            // source 是嵌套 Describable，Table 列会显示其 displayName。
            return Lists.newArrayList(
                    new ColConfig("target").setClickable(),
                    new ColConfig("source"));
        }
    }
}
