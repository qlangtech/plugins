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
import com.qlangtech.tis.plugin.MultiDescribleElementLiteria;
import com.qlangtech.tis.plugin.MultiDescribleElementSetSelector;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.transformer.UDFDesc;
import com.qlangtech.tis.plugin.ontology.Ontology;
import com.qlangtech.tis.plugin.ontology.OntologyAction;
import com.qlangtech.tis.plugin.ontology.OntologyObjectType;
import com.qlangtech.tis.plugin.ontology.OntologyProperty;
import com.qlangtech.tis.plugin.ontology.OntologyType;
import com.qlangtech.tis.plugin.ontology.impl.OntologyPluginMeta;
import com.qlangtech.tis.plugin.ontology.impl.action.DefaultOntologyAction;
import com.qlangtech.tis.plugin.ontology.impl.action.type.ActionType;
import com.qlangtech.tis.plugin.ontology.impl.action.type.ObjectActionType;
import com.qlangtech.tis.util.RelevantFieldsContextAttrValMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.Map;
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
 * @see OntologyActionRule#buildMappedValues(List, java.util.Map,java.util.Map)
 */
public class PropertyMappingConfig implements Describable<PropertyMappingConfig>,
        IPluginStore.MultiDescribleElement, MultiDescribleElementLiteria {

    /**
     * literia 描述里「映射行列表」一对的键。三条持有本行的规则
     * （{@link CreateObjectRule} / {@link ModifyObjectRule} / {@link FunctionBackedRule}）
     * 统一用它，前端 tuples 视图按同名列对齐三处映射配置。
     */
    public static final String KEY_MAPPINGS = "mappings";

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

    /**
     * 一行映射的描述：target 作键、取值来源的描述作嵌套载荷，委托给
     * {@link ValueSourceConfig#getLiteria()}（与 {@code CopyValUDF → TargetColType} 同款委托模式）。
     */
    @Override
    public List<UDFDesc> getLiteria() {
        return List.of(new UDFDesc(this.target
                , this.source == null ? java.util.Collections.emptyList() : this.source.getLiteria()));
    }

    /**
     * 映射行列表 → 描述列表，null / 空按空列表处理（Create 可以无映射行、函数可以无入参）。
     * 供 {@link CreateObjectRule}、{@link ModifyObjectRule}、{@link FunctionBackedRule} 三处共用，
     * 展开逻辑不落在任何一条规则里。
     */
    public static List<UDFDesc> literia(List<PropertyMappingConfig> mappings) {
        if (mappings == null || mappings.isEmpty()) {
            return java.util.Collections.emptyList();
        }
        return mappings.stream().map(PropertyMappingConfig::getLiteria).flatMap(List::stream).toList();
    }

    @TISExtension
    public static class DftDescriptor extends MultiDescribleElementSetSelector.MultiDescribleElementDescriptor<PropertyMappingConfig> {

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
        protected RelevantFieldsContextAttrValMap.RelevantFieldKeys createRelevantFieldKeysCreator(String toPropertyKey) {
            return new RelevantFieldsContextAttrValMap.RelevantFieldKeys(toPropertyKey) {
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
            // 2026/11/24 默认行命名空间感知：用与参数池候选聚合同一手法（loadActionDetail）
            // 取本 Action 全量对象规则，解析「属性名 → canonical 参数名」——共享属性（如主/副表
            // 都有 report_id）的默认来源直接引用先贡献规则提供的 canonical 参数，避免副表默认行
            // 引用参数池里不存在的裸名参数（旧默认行在多对象类型 Action 下全是死行）。
            // 半配置状态（创建向导中 Action 尚未完整落盘等）取不到时回退旧行为，绝不阻断映射表打开。
            Map<String, String> canonicalParams = java.util.Collections.emptyMap();
            String ownPrefix = StringUtils.EMPTY;
            OntologyPluginMeta pluginMeta = OntologyPluginMeta.createPluginMeta();
            OntologyAction action = Ontology.loadActionDetail(pluginMeta.getDomain(), pluginMeta.getPluginIdVal());
            final List<ActionType> persistedRules;
            if (action == null) {
                // 半配置状态：创建向导中 Action 尚未落盘，loadActionDetail 返回 null。按空规则列表参与
                // 推导，绝不阻断映射表打开；并集后 refs 只剩本规则的 ref，退化为单对象类型
                // （裸名默认行），与旧行为逐字一致
                persistedRules = java.util.Collections.emptyList();
            } else if (action instanceof DefaultOntologyAction ontologyAction) {
                persistedRules = ontologyAction.getActionTypes();
            } else {
                throw new IllegalStateException("action must be type of "
                        + DefaultOntologyAction.class.getSimpleName() + " but now is " + action.getClass().getName());
            }
            NamespaceCtx nsCtx = deriveNamespaceCtx(persistedRules, actionType.objectTypeReference);
            return buildCandidates(cols, selected, nsCtx.canonicalParams(), nsCtx.ownPrefix());
        }

        /**
         * 按属性列表组装映射候选行（包级静态以便单测）：每行 target = 属性名、
         * 默认来源由 {@link #defaultSource} 决定，勾选态取自已持久化的映射行。
         */
        static List<Pair<Option, IPluginStore.MultiDescribleElement>> buildCandidates(List<OntologyProperty> cols,
                                                                                      java.util.Set<String> selected,
                                                                                      Map<String, String> canonicalParams,
                                                                                      String ownPrefix) {
            List<Pair<Option, IPluginStore.MultiDescribleElement>> target = Lists.newArrayList();
            for (OntologyProperty col : cols) {
                PropertyMappingConfig config = new PropertyMappingConfig();
                config.target = col.getName();

                config.source = defaultSource(col.getName(), col.parseOntologyType(), canonicalParams, ownPrefix);
                target.add(Pair.of(
                        new Option(col.getName()).setChecked(selected.contains(col.getName())),
                        config
                ));
            }
            return target;
        }

        /**
         * 命名空间推导结果：canonical 参数映射 + 本规则前缀。纯数据载体，便于单测断言。
         */
        record NamespaceCtx(Map<String, String> canonicalParams, String ownPrefix) {
        }

        /**
         * 从「落盘规则列表 + 编辑中本规则的 ref」推导命名空间上下文（包级静态以便单测）。
         *
         * <p><b>表单态 vs 磁盘态</b>：本方法在「映射表候选生成」请求内执行，此时编辑中的规则
         * <b>尚未落盘</b>——正在给单对象规则的 Action 追加第二条对象规则时，新规则的 ref 只存在
         * 于当前解析上下文（posted host item），磁盘规则列表里没有，refs 按磁盘算会塌缩成 1、
         * 命名空间判定与保存后的最终形态不一致（默认行因此全部退化成裸名死行）。所以本规则 ref
         * 必须<b>并集</b>进 refs：已保存场景 ownRef 本就在 refs 里，并集无变化；创建向导首条规则
         * 场景 refs=[ownRef] 仍为单对象类型，自动回到旧行为。已知局限：改名单对象规则 Action 的
         * 对象类型时（磁盘旧名 + 表单新名并存）会误判成多对象类型，保存后默认行成死行——由执行期
         * {@code ObjectActionType#validatePropertyCoverage} 的 typo 硬拦兜底。
         *
         * @param persistedRules   磁盘落盘的全量规则列表（可能为空：Action 尚未落盘），可为 null 等价空列表
         * @param ownObjectTypeRef 编辑中本规则引用的对象类型（此时必非空：候选生成上游已据此加载过 cols）
         */
        static NamespaceCtx deriveNamespaceCtx(List<ActionType> persistedRules, String ownObjectTypeRef) {
            List<ActionType> rules = persistedRules == null ? java.util.Collections.emptyList() : persistedRules;
            List<String> objectTypeRefs = ObjectActionType.objectTypeReferences(rules);
            if (StringUtils.isNotEmpty(ownObjectTypeRef)
                    && !objectTypeRefs.contains(ownObjectTypeRef)) {
                objectTypeRefs = Lists.newArrayList(objectTypeRefs);
                objectTypeRefs.add(ownObjectTypeRef);
            }
            Map<String, String> canonicalParams = ObjectActionType.resolveCanonicalParams(
                    rules.stream()
                            .filter((t) -> t instanceof ObjectActionType)
                            .map((t) -> (ObjectActionType) t).toList(),
                    objectTypeRefs);
            return new NamespaceCtx(canonicalParams
                    , ObjectActionType.namespacePrefix(ownObjectTypeRef, objectTypeRefs));
        }

        /**
         * 一行映射的默认取值来源（命名空间感知，包级静态以便单测）。
         *
         * <p>多对象类型 Action 下共享属性（canonical 参数名 ≠ 本规则前缀参数名）的默认来源是
         * 引用 canonical 参数——含日期型共享属性：取主表表单值而非当前时间，正是 fill-gap
         * 执行语义的默认行形态；非共享的非日期属性引用本规则命名空间下的参数（多对象类型时
         * 带前缀，修掉旧默认行的裸名死行）；非共享日期属性与 canonical 不可用（半配置回退、
         * 单对象类型裸名）时保持旧行为逐字不变。
         */
        static ValueSourceConfig defaultSource(String propName, OntologyType propType,
                                               Map<String, String> canonicalParams, String ownPrefix) {
            if (canonicalParams != null && !canonicalParams.isEmpty()) {
                String canonical = canonicalParams.get(propName);
                if (canonical != null && !canonical.equals(ownPrefix + propName)) {
                    return new FromParameterValueSource(canonical);
                }
                boolean temporal = switch (propType) {
                    case DATE, TIMESTAMP -> true;
                    default -> false;
                };
                if (!temporal) {
                    return new FromParameterValueSource(ownPrefix + propName);
                }
            }
            return switch (propType) {
                case DATE, TIMESTAMP -> new CurrentTimeValueSource();
                default -> new FromParameterValueSource(propName);
            };
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
                    new ColConfig("target",30).setClickable(),
                     ColConfig.literiaDescCol());
        }

        @Override
        public int maxHeight() {
            return 400;
        }
    }
}
