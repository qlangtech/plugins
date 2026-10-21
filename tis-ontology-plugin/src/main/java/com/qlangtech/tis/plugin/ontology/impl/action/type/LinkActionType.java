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

package com.qlangtech.tis.plugin.ontology.impl.action.type;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.manage.common.Option;
import com.qlangtech.tis.manage.common.OptionWithEndType;
import com.qlangtech.tis.plugin.IEndTypeGetter;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ds.DataSourceFactory;
import com.qlangtech.tis.plugin.ontology.Ontology;
import com.qlangtech.tis.plugin.ontology.OntologyLinker;
import com.qlangtech.tis.plugin.ontology.OntologyObjectType;
import com.qlangtech.tis.plugin.ontology.OntologyProperty;
import com.qlangtech.tis.plugin.ontology.OntologyType;
import com.qlangtech.tis.plugin.ontology.impl.OntologyPluginMeta;
import com.qlangtech.tis.plugin.ontology.impl.action.ActionParameterConfig;
import com.qlangtech.tis.plugin.ontology.impl.action.ActionParameters;
import com.qlangtech.tis.plugin.ontology.impl.action.ObjectReferenceDataType;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.AdapterRuleVisitor;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.CreateLinkRule;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.DeleteLinkRule;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.OntologyActionRule;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.RuleGroup;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.RuleType;
import com.qlangtech.tis.plugin.ontology.impl.linker.Cardinality;
import com.qlangtech.tis.plugin.ontology.impl.linker.JoinReference;
import com.qlangtech.tis.plugin.ontology.impl.linker.LinkResources;
import com.qlangtech.tis.plugin.ontology.impl.linker.RelationshipType;
import com.qlangtech.tis.plugin.ontology.impl.linker.RelationshipTypeBackingObjectType;
import com.qlangtech.tis.plugin.ontology.impl.linker.RelationshipTypeJoinTableDataset;
import com.qlangtech.tis.plugin.ontology.impl.linker.RelationshipTypeObjectTypeForeignKeys;
import com.qlangtech.tis.plugin.ontology.impl.objtype.DataSourceBinding;
import com.qlangtech.tis.plugin.ontology.impl.objtype.ObjectTypeBinding;
import com.qlangtech.tis.util.IPluginContext;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Link Action Type - 链接操作
 * <p>
 * 对应 Ontology Link 操作，包括：
 * - CREATE_LINK: 创建链接
 * - DELETE_LINK: 删除链接
 *
 * <p><b>链接实例没有独立存储</b>：{@link OntologyLinker} 只是声明式的类型元数据，
 * 真正的链接落在对象表的外键列或连接表（join / backing object type）里。因此这里按
 * {@link RelationshipType} 把链接操作翻译成对底层表的 DML，见 {@link LinkActionExecutor}。
 *
 * <p>Action 级的 {@link #linkTypeReference} 是链接类型的唯一权威来源；规则上的
 * {@code linkTypeName} 仅为兼容旧配置保留（见 {@link #resolveLinkTypeId(String, OntologyActionRule)}）。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/7
 */
public class LinkActionType extends ActionType {

    @FormField(ordinal = 0, type = FormFieldType.ENUM, validate = {Validator.require})
    public String linkTypeReference;

    @FormField(ordinal = 1, validate = {Validator.require})
    public OntologyActionRule linkRule;

    /**
     * 把运行时表单翻译成对对象表 / 连接表的读写。
     */
    @Override
    public void executeActionRule(Describable<?> plugin) {
        executeActionRule(plugin, false);
    }

    /**
     * {@code deleteConfirmed} 对链接无意义（删除链接本身就是模态框里的显式操作），
     * 保留该重载以对齐 {@link ActionType} 的执行入口。
     */
    @Override
    public void executeActionRule(Describable<?> plugin, boolean deleteConfirmed) {
        OntologyPluginMeta meta = OntologyPluginMeta.createPluginMeta();
        OntologyActionRule rule = requireRule();
        rule.accept(new AdapterRuleVisitor(true){
            @Override
            public void visit(DeleteLinkRule rule) {

            }

            @Override
            public void visit(CreateLinkRule rule) {

            }
        });
        LinkDefinition definition = loadDefinition(meta.getDomain(), rule);
        Map<String, Object> values = ObjectActionType.getPluginPropertyVals(plugin);
        LinkActionExecutor.execute(definition, rule, values);
    }

    public LinkDefinition loadDefinition(String domain, OntologyActionRule rule) {
        String linkTypeId = resolveLinkTypeId(this.linkTypeReference, rule);
        OntologyLinker linker = OntologyLinker.load(domain, linkTypeId);
        if (linker == null) {
            throw new IllegalStateException("link type not found: '" + linkTypeId + "' (ontology:" + domain + ")");
        }
        return LinkDefinition.resolve(domain, linker.getLinkResourcesStep());
    }

    /**
     * Action 级 linkTypeReference 与规则上的旧 linkTypeName 的优先级：
     * <ol>
     *   <li>Action 级非空 → 以它为准；旧值非空但不一致则报错（避免静默按错类型执行）；</li>
     *   <li>Action 级为空、旧值非空 → 回退旧值，兼容升级前保存的 Action；</li>
     *   <li>都为空 → 报配置错误。</li>
     * </ol>
     */
    static String resolveLinkTypeId(String actionLinkTypeReference, OntologyActionRule rule) {
        String ruleLinkTypeName = ruleLinkTypeName(rule);
        if (StringUtils.isNotBlank(actionLinkTypeReference)) {
            if (StringUtils.isNotBlank(ruleLinkTypeName) && !actionLinkTypeReference.equals(ruleLinkTypeName)) {
                throw new IllegalStateException("linkTypeName '" + ruleLinkTypeName
                        + "' 与 Action 的 linkTypeReference '" + actionLinkTypeReference
                        + "' 不一致；请以 Action 的 linkTypeReference 为准，并清空规则中的旧值");
            }
            return actionLinkTypeReference;
        }
        if (StringUtils.isNotBlank(ruleLinkTypeName)) {
            return ruleLinkTypeName;
        }
        throw new IllegalStateException("未配置链接类型：请在 Action 的 linkTypeReference 中选择链接类型");
    }

    static String ruleLinkTypeName(OntologyActionRule rule) {
        if (rule instanceof CreateLinkRule createRule) {
            return createRule.linkTypeName;
        }
        if (rule instanceof DeleteLinkRule deleteRule) {
            return deleteRule.linkTypeName;
        }
        return null;
    }

    /**
     * 规则参数池里承载源对象 RID 的字段名。
     */
    static String sourceRidParamName(OntologyActionRule rule) {
        String name = null;
        if (rule instanceof CreateLinkRule createRule) {
            name = createRule.sourceObjectRidParam;
        } else if (rule instanceof DeleteLinkRule deleteRule) {
            name = deleteRule.sourceObjectRidParam;
        } else {
            throw unsupportedRule(rule);
        }
        return requireText(name, "sourceObjectRidParam");
    }

    /**
     * 规则参数池里承载目标对象 RID 的字段名。
     */
    static String targetRidParamName(OntologyActionRule rule) {
        String name = null;
        if (rule instanceof CreateLinkRule createRule) {
            name = createRule.targetObjectRidParam;
        } else if (rule instanceof DeleteLinkRule deleteRule) {
            name = deleteRule.targetObjectRidParam;
        } else {
            throw unsupportedRule(rule);
        }
        return requireText(name, "targetObjectRidParam");
    }

    private static IllegalStateException unsupportedRule(OntologyActionRule rule) {
        return new IllegalStateException("unsupported link rule: "
                + (rule == null ? "null" : rule.getClass().getName()));
    }

    private OntologyActionRule requireRule() {
        return Objects.requireNonNull(this.linkRule, "linkRule can not be null");
    }

    /**
     * 供 {@code resolveRuntimeForm} 回传的链接运行时元信息：源/目标对象类型、两个 RID 字段名，
     * 以及目标对象选择器用来渲染 label 的属性。
     *
     * <p>参数池缺参数时直接抛错，前端据此在打开模态框时就禁用提交，而不是等用户点提交后
     * 收到后端的「can not find parameter」。
     */
    public JSONObject resolveLinkMetadata(String domain, ActionParameters paramPool) {
        OntologyActionRule rule = requireRule();
        String linkTypeId = resolveLinkTypeId(this.linkTypeReference, rule);
        LinkDefinition definition = loadDefinition(domain, rule);
        String sourceParam = sourceRidParamName(rule);
        String targetParam = targetRidParamName(rule);

        Set<String> parameterIds = paramPool == null || paramPool.parameters == null
                ? Set.of()
                : paramPool.parameters.stream()
                .map(ActionParameterConfig::identityValue)
                .collect(Collectors.toCollection(LinkedHashSet::new));
        List<String> missing = new ArrayList<>();
        if (!parameterIds.contains(sourceParam)) {
            missing.add(sourceParam);
        }
        if (!parameterIds.contains(targetParam)) {
            missing.add(targetParam);
        }
        if (!missing.isEmpty()) {
            throw new IllegalStateException("Action 参数池缺少链接参数：" + missing
                    + "，请先在 Action 详情页添加对应参数");
        }

        JSONObject result = new JSONObject();
        result.put("linkTypeName", linkTypeId);
        result.put("relationshipType", definition.relationshipType().name());
        result.put("sourceObjectType", definition.source().objectType());
        result.put("sourceRidFieldName", sourceParam);
        result.put("targetObjectType", definition.target().objectType());
        result.put("targetRidFieldName", targetParam);
        result.put("targetDisplayProperty", resolveDisplayProperty(definition.target().ontologyObjectType()));
        return result;
    }

    /**
     * 目标对象选择器的展示列：优先常见名称字段，其次任意 STRING 属性，最后主键。
     */
    private static String resolveDisplayProperty(OntologyObjectType objectType) {
        List<OntologyProperty> cols = objectType.getCols();
        List<String> preferred = List.of("name", "title", "displayname", "display_name", "label", "code");
        for (String candidate : preferred) {
            for (OntologyProperty col : cols) {
                if (candidate.equalsIgnoreCase(col.getName())) {
                    return col.getName();
                }
            }
        }
        for (OntologyProperty col : cols) {
            try {
                if (col.parseOntologyType() == OntologyType.STRING) {
                    return col.getName();
                }
            } catch (Exception ignored) {
                // 单个属性的类型解析失败不影响选择器；继续找下一个
            }
        }
        if (objectType.hasDisablePK()) {
            return null;
        }
        return objectType.getPk().map(OntologyProperty::getName).orElse(null);
    }

    /**
     * 配置参数池时，为链接 Action 自动提供 sourceRid / targetRid 两个对象引用参数。
     */
    @Override
    public List<Pair<Option, ActionParameterConfig>> getEnumableCandidateSet(ActionParameters actionParams) {
        OntologyActionRule rule = getRule();
        if (rule == null) {
            return List.of();
        }
        try {
            resolveLinkTypeId(this.linkTypeReference, rule);
        } catch (IllegalStateException e) {
            // 还在配置链接类型，暂不提供候选
            return List.of();
        }
        OntologyPluginMeta meta = OntologyPluginMeta.createPluginMeta();
        LinkDefinition definition = loadDefinition(meta.getDomain(), rule);
        String sourceParam = sourceRidParamName(rule);
        String targetParam = targetRidParamName(rule);

        Set<String> selected = actionParams == null || actionParams.parameters == null
                ? Set.of()
                : actionParams.parameters.stream()
                .map(ActionParameterConfig::identityValue)
                .collect(Collectors.toSet());

        List<Pair<Option, ActionParameterConfig>> result = Lists.newArrayList();
        result.add(Pair.of(
                new Option(sourceParam).setChecked(selected.contains(sourceParam)),
                createObjectRefParam(sourceParam, "源对象 RID", 0, definition.source().objectType())));
        result.add(Pair.of(
                new Option(targetParam).setChecked(selected.contains(targetParam)),
                createObjectRefParam(targetParam, "目标对象 RID", 1, definition.target().objectType())));
        return result;
    }

    private static ActionParameterConfig createObjectRefParam(String parameterId, String displayName,
                                                              int order, String objectTypeId) {
        ActionParameterConfig config = new ActionParameterConfig();
        config.parameterId = parameterId;
        config.displayName = displayName;
        config.order = order;
        config.hidden = Boolean.FALSE;
        ObjectReferenceDataType dataType = new ObjectReferenceDataType();
        dataType.objectTypeId = objectTypeId;
        config.dataType = dataType;
        return config;
    }

    /**
     * 链接类型下拉选项。与 {@code LinkReference.getObjectTypeOpts()} 一样从线程局部取 ontology domain。
     * 单个 linker 配置损坏时跳过，避免整个下拉不可用。
     */
    public static List<OptionWithEndType> getLinkTypeOpts() {
        IPluginContext pluginContext = IPluginContext.getThreadLocalInstance();
        OntologyPluginMeta meta = OntologyPluginMeta.createPluginMeta(pluginContext.getContext());
        List<OptionWithEndType> opts = Lists.newArrayList();
        for (OntologyLinker linker : Ontology.loadAllLinkers(meta.getDomain())) {
            try {
                String linkTypeId = linker.identityValue();
                OptionWithEndType opt = new OptionWithEndType(linkTypeId, linkTypeId, IEndTypeGetter.EndType.Link);
                opt.setDescription(describeLinker(meta.getDomain(), linker));
                opts.add(opt);
            } catch (Exception e) {
                // 该 linker 的 steps 不完整，identityValue / getLinkResourcesStep 都会抛；
                // 引用了它也无法执行，故不出现在下拉里
            }
        }
        return opts;
    }

    private static String describeLinker(String domain, OntologyLinker linker) {
        try {
            return LinkDefinition.resolve(domain, linker.getLinkResourcesStep()).describe();
        } catch (Exception e) {
            return "链接类型配置不完整：" + (e.getMessage() == null ? e.getClass().getName() : e.getMessage());
        }
    }

    @Override
    public RuleGroup getRuleGroup() {
        return RuleGroup.ONTOLOGY_LINK;
    }

    @Override
    public List<RuleType> getAllowedRuleTypes() {
        return List.of(
                RuleType.CREATE_LINK,
                RuleType.DELETE_LINK
        );
    }

    @Override
    public OntologyActionRule getRule() {
        return linkRule;
    }

    /**
     * 链接的物理落表描述。
     *
     * <p>{@code source}/{@code target} 的 {@link TableRef#pk()} 用来按 RID 定位对象行；
     * {@code sourceField}/{@code targetField} 是该行上参与链接值的列。二者可能不同
     * （例如 JoinTableDataset 允许用非主键的唯一列做连接）。
     *
     * @param join             FK 类型为 null，其余为连接表
     * @param joinSourceField  连接表上存源端值的列
     * @param joinTargetField  连接表上存目标端值的列
     * @param cardinality      仅 FK 类型有
     */
    public record LinkDefinition(RelationshipType relationshipType,
                                 TableRef source, String sourceField,
                                 TableRef target, String targetField,
                                 TableRef join, String joinSourceField, String joinTargetField,
                                 Cardinality cardinality) {

        public String describe() {
            String base = source.objectType() + "." + sourceField + " ↔ "
                    + target.objectType() + "." + targetField;
            return join == null ? base : base + "（连接表：" + join.objectType() + "）";
        }

        public static LinkDefinition resolve(String domain, LinkResources resources) {
            Objects.requireNonNull(resources, "link resources can not be null");
            RelationshipType relationshipType = resolveRelationshipType(resources);
            return switch (relationshipType) {
                case ObjectTypeForeignKeys -> resolveForeignKeys(domain, (RelationshipTypeObjectTypeForeignKeys) resources);
                case JoinTableDataset -> resolveJoinTableDataset(domain, (RelationshipTypeJoinTableDataset) resources);
                case BackingObjectType -> resolveBackingObjectType(domain, (RelationshipTypeBackingObjectType) resources);
            };
        }

        private static RelationshipType resolveRelationshipType(LinkResources resources) {
            if (!(resources.getDescriptor() instanceof LinkResources.BasicLinkResourceDesc desc)) {
                String actual = resources.getDescriptor() == null
                        ? "null" : resources.getDescriptor().getClass().getName();
                throw new IllegalStateException("link resources descriptor must be "
                        + LinkResources.BasicLinkResourceDesc.class.getName() + " but actual is " + actual);
            }
            return desc.getRelationShipType();
        }

        private static LinkDefinition resolveForeignKeys(String domain, RelationshipTypeObjectTypeForeignKeys fk) {
            if (fk.left == null || fk.right == null) {
                throw new IllegalStateException("foreign-key link must define both left and right");
            }
            TableRef source = resolveTable(domain, fk.left.getObjectType(), true, "left");
            TableRef target = resolveTable(domain, fk.right.getObjectType(), true, "right");
            Cardinality cardinality = StringUtils.isBlank(fk.cardinality)
                    ? null : Cardinality.valueOf(fk.cardinality.trim());
            return new LinkDefinition(RelationshipType.ObjectTypeForeignKeys,
                    source, requireText(fk.left.targetField, "left.targetField"),
                    target, requireText(fk.right.targetField, "right.targetField"),
                    null, null, null, cardinality);
        }

        private static LinkDefinition resolveJoinTableDataset(String domain, RelationshipTypeJoinTableDataset jt) {
            if (jt.left == null || jt.right == null) {
                throw new IllegalStateException("join-table link must define both left and right");
            }
            if (!(jt.join instanceof JoinReference join)) {
                throw new IllegalStateException("join must be type of " + JoinReference.class.getName());
            }
            TableRef source = resolveTable(domain, jt.left.getObjectType(), true, "left");
            TableRef target = resolveTable(domain, jt.right.getObjectType(), true, "right");
            TableRef joinTable = resolveTable(domain, join.getObjectType(), false, "join");
            return new LinkDefinition(RelationshipType.JoinTableDataset,
                    source, requireText(jt.left.targetField, "left.targetField"),
                    target, requireText(jt.right.targetField, "right.targetField"),
                    joinTable, requireText(join.targetField, "join.targetField"),
                    requireText(join.rightTargetField, "join.rightTargetField"), null);
        }

        private static LinkDefinition resolveBackingObjectType(String domain, RelationshipTypeBackingObjectType bo) {
            if (!(bo.joinObjectType instanceof JoinReference join)) {
                throw new IllegalStateException("joinObjectType must be type of " + JoinReference.class.getName());
            }
            TableRef source = resolveTable(domain, requireText(bo.leftObjectType, "leftObjectType"), true, "left");
            TableRef target = resolveTable(domain, requireText(bo.rightObjectType, "rightObjectType"), true, "right");
            TableRef joinTable = resolveTable(domain, join.getObjectType(), false, "join");
            // BackingObjectType 的两端连接值固定为左右对象的 PK（与 getLinks() 的 inferPk 一致），
            // 但这里用显式 domain 推断，不依赖线程局部上下文
            return new LinkDefinition(RelationshipType.BackingObjectType,
                    source, source.pk(), target, target.pk(),
                    joinTable, requireText(join.targetField, "join.targetField"),
                    requireText(join.rightTargetField, "join.rightTargetField"), null);
        }

        static TableRef resolveTable(String domain, String objectTypeName, boolean requirePk, String endLabel) {
            String otName = requireText(objectTypeName, endLabel + ".objectType");
            OntologyObjectType objectType = Ontology.loadObjectTypeDetail(domain, otName);
            if (objectType == null) {
                throw new IllegalStateException("object type not found: '" + otName
                        + "' (ontology:" + domain + ")");
            }
            ObjectTypeBinding binding = objectType.getDataSourceBinding();
            if (binding == null || !binding.hasBound()) {
                throw new IllegalStateException("object type '" + otName + "' has not bind dataSource");
            }
            String physicalTable = binding instanceof DataSourceBinding dataSourceBinding
                    ? dataSourceBinding.resolvePhysicalTableName(otName) : otName;
            String pk = null;
            if (requirePk) {
                if (objectType.hasDisablePK()) {
                    throw new IllegalStateException("object type '" + otName + "' has disabled PK");
                }
                pk = objectType.getPk().map(OntologyProperty::getName)
                        .orElseThrow(() -> new IllegalStateException(
                                "object type '" + otName + "' has no PK column"));
            }
            return new TableRef(otName, objectType, binding.getDataSource(), physicalTable, pk);
        }
    }

    /**
     * 一张物理表的定位信息；{@code pk} 仅在需要按 RID 定位行时非空（join 表不需要）。
     */
    public record TableRef(String objectType, OntologyObjectType ontologyObjectType,
                           DataSourceFactory dataSource, String physicalTable, String pk) {
    }

    private static String requireText(String value, String field) {
        if (StringUtils.isBlank(value)) {
            throw new IllegalStateException(field + " can not be empty");
        }
        return value.trim();
    }

    @TISExtension
    public static class DefaultDescriptor extends BaseActionTypeDescriptor {

        @Override
        public RuleGroup getRuleGroup() {
            return RuleGroup.ONTOLOGY_LINK;
        }

//        @Override
//        public String getDisplayName() {
//            return "Link";
//        }
    }
}
