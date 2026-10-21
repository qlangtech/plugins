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

import com.alibaba.citrus.turbine.Context;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.MultiStepsSupportHostDescriptor;
import com.qlangtech.tis.extension.OneStepOfMultiSteps;
import com.qlangtech.tis.extension.OneStepOfMultiSteps.Step;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.impl.PropValRewrite;
import com.qlangtech.tis.plugin.ontology.Ontology;
import com.qlangtech.tis.plugin.ontology.OntologyAction;
import com.qlangtech.tis.plugin.ontology.impl.OntologyPluginMeta;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.DeleteObjectRule;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.ModifyObjectRule;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.OntologyActionRule;
import com.qlangtech.tis.plugin.ontology.impl.action.type.ActionType;
import com.qlangtech.tis.plugin.ontology.impl.action.type.LinkActionType;
import com.qlangtech.tis.plugin.ontology.impl.action.type.ObjectActionType;
import com.qlangtech.tis.runtime.module.misc.FormVaildateType;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.util.AttrValMap;
import com.qlangtech.tis.util.DefaultDescriptorsJSON;
import com.qlangtech.tis.util.DescribableJSON;
import com.qlangtech.tis.util.IPluginContext;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * 默认的 Ontology Action Type 实现
 * <p>
 * 定义对象的操作类型，包括：
 * - 写操作（创建、修改、删除对象/链接）
 * - 副作用操作（通知、Webhook）
 * - 高级操作（函数支持、场景合并、数据构建触发）
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/6
 * @see ActionMetadata
 * @see ActionRuleSetter
 * @see ActionParameters
 * @see ActionSubmissionCriteria
 */
@SuppressWarnings("all")
public class DefaultOntologyAction extends OntologyAction {

    @Override
    public String identityValue() {
        // 从第一步获取名称作为标识
        return getName();
    }

    @Override
    public void manipuldateProcess(IPluginContext pluginContext,
                                   com.qlangtech.tis.util.UploadPluginMeta pluginMeta,
                                   java.util.Optional<com.alibaba.citrus.turbine.Context> context) {
        // 可以在这里添加自定义持久化逻辑
        // 例如：验证跨步骤的业务规则、生成默认值等
    }

    /**
     * 获取第一步（Metadata）
     */
    private ActionMetadata getMetadata() {
        return (ActionMetadata) getMultiStepsSavedItems()[0];
    }

    /**
     * 获取第二步（Action Types）
     */
    private ActionRuleSetter getRuleSetter() {
        return (ActionRuleSetter) getMultiStepsSavedItems()[1];
    }

    /**
     * 获取所有配置的 ActionType 列表（委托给 ActionRuleSetter）
     */
    public ActionType getActionType() {
        ActionRuleSetter ruleSetter = getRuleSetter();
        return ruleSetter.actionType;
    }

    /**
     * 对象身份（rid）参数名：修改/删除类动作据此定位目标对象，返回对应表单字段名；
     * 创建类动作返回 null。仅供 {@link DefaultDesc#httpProcess} 的 {@code resolveRuntimeForm}
     * 分支回传字段名给前端预填，执行期仍由 {@code ObjectActionType#resolveRowIdentity} 精确解析。
     */
    public String resolveObjectIdentityParam(OntologyPluginMeta meta) {
        ActionType actionType = getActionType();
        OntologyActionRule rule = actionType.getRule();
        if (actionType instanceof ObjectActionType objectActionType) {
            if (rule instanceof ModifyObjectRule modifyRule) {
                return ObjectActionType.resolveIdentityParameterName(
                        objectActionType.loadObjectType(meta.getDomain()), modifyRule.ridParameterName);
            }
            if (rule instanceof DeleteObjectRule deleteRule) {
                return ObjectActionType.resolveIdentityParameterName(
                        objectActionType.loadObjectType(meta.getDomain()), deleteRule.ridParameterName);
            }
        }
        return null;
    }

    /**
     * 按 rid 加载完整对象，供 Workshop 运行时表单预填。
     *
     * <p>只有「修改 / 删除对象」类规则需要对象身份；创建类动作返回 null。
     * 委托给 {@link ObjectActionType#loadObjectByRid(String, String, Object)}，与执行期
     * {@code resolveRowIdentity} 使用同一条身份列解析规则。
     *
     * @param meta 当前 Action 的插件元信息（提供 ontology domain）
     * @param rid  行身份值
     * @return 对象属性 → 值；不适用或没有匹配行时返回 null
     */
    public Map<String, Object> loadObjectByRid(OntologyPluginMeta meta, String rid) {
        ActionType actionType = getActionType();
        if (!(actionType instanceof ObjectActionType objectActionType)) {
            return null;
        }
        OntologyActionRule rule = actionType.getRule();
        if (rule instanceof ModifyObjectRule modifyRule) {
            return objectActionType.loadObjectByRid(meta.getDomain(), modifyRule.ridParameterName, rid);
        }
        if (rule instanceof DeleteObjectRule deleteRule) {
            return objectActionType.loadObjectByRid(meta.getDomain(), deleteRule.ridParameterName, rid);
        }
        return null;
    }

//    /**
//     * 获取指定类型的 ActionType 列表
//     */
//    public <T extends ActionType> List<T> getActionTypes(Class<T> type) {
//        return getActionTypes().stream()
//                .filter(type::isInstance)
//                .map(type::cast)
//                .collect(Collectors.toList());
//    }
//
//    /**
//     * 获取所有启用的规则
//     */
//    public List<com.qlangtech.tis.plugin.ontology.impl.action.rule.OntologyActionRule> getAllRules() {
//        return getActionTypes().stream()
//                .map(ActionType::getRule)
//                .filter(java.util.Objects::nonNull)
//                .filter(OntologyActionRule::isEnabled)
//                .collect(Collectors.toList());
//    }

    /**
     * 获取第四步（提交条件）
     */
    private ActionSubmissionCriteria getSubmissionCriteriaStep() {
        return (ActionSubmissionCriteria) getMultiStepsSavedItems()[Step.Step4.getStepIndex()];
    }

    @Override
    public String getName() {
        return getMetadata().name;
    }

    @Override
    public String getDisplayName() {
        return getMetadata().displayName;
    }

    @Override
    public String getDescription() {
        return getMetadata().description;
    }

//    @Override
//    public String getPrimaryType() {
//        ActionType actionType = getActionType();
//
//        return actionTypes.get(0).getDescriptor().getDisplayName();
//    }

//    /**
//     * 获取参数池（已结构化的参数定义列表）
//     */
//    public List<ActionParameterConfig> getParameterConfigs() {
//        return getParameters().getParameters();
//    }

    /**
     * 获取提交条件（多条之间为「且」关系；空列表表示不限制提交）
     */
    public List<SubmissionCriterion> getSubmissionCriteria() {
        return getSubmissionCriteriaStep().getCriteria();
    }

    @Override
    public java.util.List<com.qlangtech.tis.plugin.datax.transformer.UDFDesc> getLiteria() {
        java.util.List<com.qlangtech.tis.plugin.datax.transformer.UDFDesc> literia =
                com.google.common.collect.Lists.newArrayList();

        literia.add(new com.qlangtech.tis.plugin.datax.transformer.UDFDesc("Name", getName()));
        literia.add(new com.qlangtech.tis.plugin.datax.transformer.UDFDesc("Display Name", getDisplayName()));
        //literia.add(new com.qlangtech.tis.plugin.datax.transformer.UDFDesc("Target Object Type", getTargetObjectType()));
        ActionType actionType = getActionType();

        literia.add(new com.qlangtech.tis.plugin.datax.transformer.UDFDesc("Action Type", actionType.getRuleGroup().getDisplayName()));

        return literia;
    }

    @TISExtension
    public static class DefaultDesc extends Ontology.BasicDesc
            implements MultiStepsSupportHostDescriptor<OntologyAction> {

        @Override
        public Class<OntologyAction> getHostClass() {
            return OntologyAction.class;
        }

        @Override
        public List<OneStepOfMultiSteps.BasicDesc> getStepDescriptionList() {
            return List.of(
                    new ActionMetadata.Desc(),
                    new ActionRuleSetter.Desc()
            );
        }

        /**
         * 参数数据类型标识。与 {@link PrimitiveDataType.DftDescriptor#getDisplayName()} /
         * {@link ObjectReferenceDataType.DftDescriptor#getDisplayName()} 逐字一致，
         * 前端直接展示即可，不必再翻译。
         */
        private static final String DATA_TYPE_PRIMITIVE = "Primitive_Type";
        private static final String DATA_TYPE_OBJECT_REFERENCE = "Object_Reference";

        /**
         * 返回 Action 详情，供本体详情页的 Action Type 子页面（第二阶段编辑）使用。
         *
         * <p>响应体形状对应前端 {@code OntologyActionDetail}：
         * <pre>
         * {name, displayName, description, objectType, domain, createTime,
         *  rules: [{ruleType, target, properties[]}],
         *  parameters: [{parameterId, displayName, dataType, required, hidden}],
         *  submissionCriteria: []}
         * </pre>
         *
         * <p><b>参数池由规则推导，但允许微调后落盘</b>：创建向导已收敛为两步
         * （见 {@link #getStepDescriptionList()}），参数池步骤不在其中，
         * {@code getMultiStepsSavedItems()[2]} 会越界。因此这里先遍历各规则的映射行与
         * 规则自带的参数名（rid/sourceRid/recipient 等），把规则引用到的参数名推导成参数池；
         * 再交由 {@link ActionParameters#resolveNewParams} 校对：推导出来但还没落盘的参数
         * 补写一份 {@link ActionParameterConfig}，已落盘的则以存量配置（显示名/数据类型）为准。
         * 同理 {@code submissionCriteria} 恒为空列表，语义是「不限制提交」。
         */
        @Override
        public void httpProcess(IControlMsgHandler paramGetter, IPluginContext pluginContext, Context context) throws Exception {
            String methodType = paramGetter.getString("type");
            String ontologyName = paramGetter.getString("ontology");
            String actionName = paramGetter.getString("actionName");
            if (StringUtils.isEmpty(methodType)) {
                throw new IllegalArgumentException("param methodType can not be empty");
            }
            if (org.apache.commons.lang3.StringUtils.isEmpty(ontologyName)) {
                throw new IllegalArgumentException("param 'ontology' can not be empty");
            }
            if (org.apache.commons.lang3.StringUtils.isEmpty(actionName)) {
                throw new IllegalArgumentException("param 'actionName' can not be empty");
            }
            OntologyAction action = Ontology.loadActionDetail(ontologyName, actionName);
            if (action == null) {
                throw new IllegalArgumentException("Action '" + actionName + "' not found in ontology '" + ontologyName + "'");
            }
            if (!(action instanceof DefaultOntologyAction defaultAction)) {
                throw new IllegalStateException("Action must be instance of DefaultOntologyAction");
            }
            ActionType actionType = Objects.requireNonNull(defaultAction.getActionType()
                    , "actionName:" + actionName + " relevant action type can not be null");
            OntologyPluginMeta paramMeta = OntologyPluginMeta.create(Ontology.OntologyEnum.Action, ontologyName)
                    .setPluginIdVal(actionName)
                    .setPersistence();

            switch (methodType) {
                case "getActionRelevant" -> {
                    com.alibaba.fastjson.JSONObject result = new com.alibaba.fastjson.JSONObject();
                    Map<String, Boolean> paramRefs = new LinkedHashMap<>();

                    OntologyActionRule rule = actionType.getRule();
                    List<String> properties = new ArrayList<>();
                    //  collectRuleRefs(rule, properties, paramRefs);


                    ActionParameters paramPool = ActionParameters.loadParameters(paramMeta);

                    result.put("descriptors", new DefaultDescriptorsJSON(TIS.get().getDescriptor(ActionParameters.class)).getDescriptorsJSON());
                    if (paramPool != null) {
                        result.put("parameters", new DescribableJSON(paramPool).getItemJson());
                        result.put("previewDescriptors", ActionFormDescriptorBuilder.build(paramMeta, paramPool));
                    }
                    result.put("submissionCriteria", new com.alibaba.fastjson.JSONArray());

                    paramGetter.setBizResult(context, result);
                }
                case "resolveActionForm" -> {
                    JSONObject post = pluginContext.getJSONPostContent();
                    // whitelist 必须是全部字段名（parameters / submitButton / successMessage），
                    // 否则没进 whitelist 的字段会静默读成 null（见 AttrValMap.createRelevantFieldsContext）。
                    AttrValMap.RelevantFieldsContextAttrValMap<ActionParameters> attrs =
                            AttrValMap.createRelevantFieldsContext(paramGetter, context, post,
                                    (desc) -> new ArrayList<>(desc.getPropertyTypes().keySet()));
                    if (!attrs.validate().isValid()) {
                        return;
                    }
                    ActionParameters params = attrs.createPluginInstance();

                    com.alibaba.fastjson.JSONObject result = new com.alibaba.fastjson.JSONObject();
                    // 与 getActionRelevant 分支同构：把「参数池 → 扁平运行时表单」推导成的 descriptor 回传，
                    // 前端据此驱动右侧「表单预览」。
                    result.put("previewDescriptors", ActionFormDescriptorBuilder.build(paramMeta, params));
                    params.parameters = null;

                    result.put(ActionParameters.KEY_PARAMETERS, new DescribableJSON(params).getItemJson());

                    paramGetter.setBizResult(context, result);
                }
                case "executeActionInPreview" -> {
                    /**
                     * 在review模式下执行action
                     */
                    // 执行用户填写的表单内容。预览表单是运行时生成的 ActionFormPreview，
                    // 不在 Sezpoz 扩展索引里，不能按 impl 全局反查 descriptor，
                    // 须由当前 Action 已持久化的参数池现场生成。
                    JSONObject post = pluginContext.getJSONPostContent();
                    JSONObject previewForm = Objects.requireNonNull(
                            post.getJSONObject("previewItem"), "previewItem can not be null");
                    JSONObject paramsItem = Objects.requireNonNull(
                            post.getJSONObject("paramsItem"), "paramsItem can not be null");

                    AttrValMap.RelevantFieldsContextAttrValMap<ActionParameters> attrs =
                            AttrValMap.createRelevantFieldsContext(paramGetter, context, paramsItem,
                                    (desc) -> Lists.newArrayList(ActionParameters.KEY_PARAMETERS));

                    if (!attrs.validate().isValid()) {
                        throw new IllegalStateException("attrs.validate() must be vaild");
                    }

                    ActionParameters paramPool = attrs.createPluginInstance();// ActionParameters.loadParameters(paramMeta);
                    if (paramPool == null) {
                        paramGetter.addErrorMessage(context, "Action '" + actionName + "' 尚未配置参数池");
                        return;
                    }
                    // 详情页预览不接删除确认：删除只能从 Workshop 模态框的危险按钮发起
                    executePreviewForm(actionType, paramMeta, paramPool, previewForm, false, paramGetter, context);
                }
                case "executeAction" -> {
                    // workshop 按钮组提交运行时表单执行。与 executeActionRule 分支的差异：
                    // 参数池已在落盘时定稿，这里从磁盘加载（无 paramsItem）；表单投影也只剩 previewItem
                    // （对象身份 rid 已由前端 prefill 进表单字段，随 previewItem 一起提交）。
                    JSONObject post = pluginContext.getJSONPostContent();
                    JSONObject previewForm = Objects.requireNonNull(
                            post.getJSONObject("previewItem"), "previewItem can not be null");
                    // 删除确认标记是 previewItem 的兄弟字段：运行时表单字段由参数池推导，
                    // 塞进 vals 会被 STRICT 校验丢弃，故只能走顶层
                    boolean deleteConfirmed = post.getBooleanValue("deleteConfirmed");
                    ActionParameters paramPool = ActionParameters.loadParameters(paramMeta);
                    if (paramPool == null) {
                        paramGetter.addErrorMessage(context, "Action '" + actionName + "' 尚未配置参数池");
                        return;
                    }
                    executePreviewForm(actionType, paramMeta, paramPool, previewForm, deleteConfirmed,
                            paramGetter, context);
                }
                case "resolveRuntimeForm" -> {
                    // workshop 按钮组打开运行时表单：参数池已落盘，直接加载并现场生成
                    // 「参数池 → 扁平运行时表单」的 descriptor 回传（与 resolveActionForm 同构），
                    // 再附上对象身份字段名供前端预填选中对象的 rid。
                    com.alibaba.fastjson.JSONObject result = new com.alibaba.fastjson.JSONObject();
                    ActionParameters paramPool = ActionParameters.loadParameters(paramMeta);
                    result.put("previewDescriptors",
                            paramPool == null ? null : ActionFormDescriptorBuilder.build(paramMeta, paramPool));
                    result.put("ridFieldName", defaultAction.resolveObjectIdentityParam(paramMeta));
                    // 规则类型决定前端模态框的提交语义：DELETE_OBJECT 时换成危险按钮并携带
                    // deleteConfirmed，其余规则保持普通「提交」
                    OntologyActionRule rule = actionType.getRule();
                    result.put("ruleType", rule == null ? null : rule.getRuleType().name());

                    // 链接类 Action：回传源/目标对象类型与两个 RID 字段名，前端据此预填源对象、
                    // 渲染目标对象选择器。配置不完整（缺参数 / 链接类型坏）时只回传错误，
                    // 不能连坐整个表单的加载。
                    if (actionType instanceof LinkActionType linkActionType) {
                        try {
                            result.put("linkInfo",
                                    linkActionType.resolveLinkMetadata(ontologyName, paramPool));
                        } catch (Exception e) {
                            result.put("linkConfigError", e.getMessage() == null
                                    ? e.getClass().getName() : e.getMessage());
                        }
                    }

                    // 可选：前端带上选中行的 rid 时，一次性回传完整对象用于预填全部字段。
                    // 查询失败只作为 objectError 回传，不能连坐整个表单的加载。
                    String rid = paramGetter.getString("rid");
                    if (StringUtils.isNotEmpty(rid)) {
                        try {
                            result.put("object", defaultAction.loadObjectByRid(paramMeta, rid));
                        } catch (Exception e) {
                            result.put("objectError", e.getMessage() == null
                                    ? e.getClass().getName() : e.getMessage());
                        }
                    }
                    paramGetter.setBizResult(context, result);
                }

                default -> throw new IllegalArgumentException("illegal methodType:" + methodType);
            }


        }

//        /**
//         * 把一个 {@link CustomizeContent} 写进响应体。
//         *
//         * <p>没启用定制（{@code "off"}）或启用后内容为空时<b>不写这个键</b>，而不是下发空串 ——
//         * 前端按「键在不在」区分「用户定制过」与「没定制」，据此回退到缺省文案。
//         */
//        private static void putCustomizeContent(com.alibaba.fastjson.JSONObject result,
//                                                String key, CustomizeContent customize) {
//            if (customize != null) {
//                customize.content().ifPresent((content) -> result.put(key, content));
//            }
//        }

//        /**
//         * 规则作用的目标：对象规则取对象类型名，链接规则取链接类型名，
//         * 其余（函数、副作用、高级）没有统一的目标字段，退回 ActionType 的显示名。
//         */
//        private static String resolveRuleTarget(ActionType actionType) {
//            if (actionType instanceof ObjectActionType objectActionType) {
//                return objectActionType.objectTypeReference;
//            }
//            if (actionType instanceof LinkActionType linkActionType) {
//                return linkActionType.linkTypeReference;
//            }
//            return actionType.getDescriptor().getDisplayName();
//        }

//        /**
//         * 从单条规则中收集：该规则会写哪些字段（前端 Rules 表的 properties 列），
//         * 以及它引用了哪些 Action 参数（前端 Parameters 表的行来源）。
//         *
//         * <p>未知规则类型不做处理、也不抛异常 —— 新增规则实现时这里最多是「少推几个参数」，
//         * 不应该让整个详情页打不开。
//         *
//         * @param properties 出参，追加规则作用的字段名
//         * @param paramRefs  出参，参数名 → 是否对象引用（重复引用同一参数时取「或」）
//         */
//        private static void collectRuleRefs(OntologyActionRule rule,
//                                            List<String> properties,
//                                            Map<String, Boolean> paramRefs) {
//            if (rule == null) {
//                return;
//            }
//
//            if (rule instanceof CreateObjectRule createRule) {
//                collectMappings(createRule.propertyMappings, properties, paramRefs);
//                // 自动生成 RID 时不需要用户提供 rid 参数
//                if (!Boolean.TRUE.equals(createRule.generateRid)) {
//                    addParam(paramRefs, "rid", true);
//                }
//            } else if (rule instanceof ModifyObjectRule modifyRule) {
//                collectMappings(modifyRule.propertyMappings, properties, paramRefs);
//                addParam(paramRefs, modifyRule.ridParameterName, true);
//            } else if (rule instanceof DeleteObjectRule deleteRule) {
//                addParam(paramRefs, deleteRule.ridParameterName, true);
//            } else if (rule instanceof CreateLinkRule createLinkRule) {
//                addParam(paramRefs, createLinkRule.sourceObjectRidParam, true);
//                addParam(paramRefs, createLinkRule.targetObjectRidParam, true);
//            } else if (rule instanceof DeleteLinkRule deleteLinkRule) {
//                addParam(paramRefs, deleteLinkRule.sourceObjectRidParam, true);
//                addParam(paramRefs, deleteLinkRule.targetObjectRidParam, true);
//            } else if (rule instanceof FunctionBackedRule functionRule) {
//                // 函数规则的映射行 target 是函数形参名，同样计入「作用字段」供前端展示
//                collectMappings(functionRule.parameterMappings, properties, paramRefs);
//            } else if (rule instanceof NotificationRule notificationRule) {
//                addParam(paramRefs, notificationRule.recipientParam, false);
//            }
//            // WebhookRule 没有参数引用，无需处理
//        }

//        /**
//         * 遍历「目标字段 ← 取值来源」映射行：target 计入作用字段，
//         * 取值来源为 {@link FromParameterValueSource} 时其参数名计入参数池。
//         *
//         * <p>映射行里引用的参数一律按基本类型推断 —— 只有 rid 这类语义明确的位置
//         * 才敢判定为对象引用。
//         */
//        private static void collectMappings(List<PropertyMappingConfig> mappings,
//                                            List<String> properties,
//                                            Map<String, Boolean> paramRefs) {
//            if (mappings == null) {
//                return;
//            }
//            for (PropertyMappingConfig mapping : mappings) {
//                if (mapping == null) {
//                    continue;
//                }
//                if (org.apache.commons.lang3.StringUtils.isNotEmpty(mapping.target)) {
//                    properties.add(mapping.target);
//                }
//                if (mapping.source instanceof FromParameterValueSource parameterSource) {
//                    addParam(paramRefs, parameterSource.parameterRef, false);
//                }
//            }
//        }

//        /**
//         * 登记一个被规则引用的参数。同名参数被多处引用时取「或」——
//         * 只要有一处把它当对象引用，就按对象引用展示。
//         */
//        private static void addParam(Map<String, Boolean> paramRefs, String paramName, boolean objectReference) {
//            if (org.apache.commons.lang3.StringUtils.isEmpty(paramName)) {
//                return;
//            }
//            paramRefs.merge(paramName, objectReference, (existing, current) -> existing || current);
//        }

        /**
         * 把参数池转成前端 {@code ActionParameter} 数组。
         *
         * <p>显示名与数据类型都取存量配置（{@link ActionParameterConfig}），不再由这里硬编码 ——
         * 参数池现在是「规则自动生成 + 允许微调」，用户调过的值就存在这些配置里。
         *
         * <p>{@code required}/{@code hidden} 仍按老口径写死 {@code true}/{@code false}：它们是前端
         * {@code ActionParameter} 契约里的字段，目前没有与之对应的存量字段
         * （{@code required} 真正落在 {@link PrimitiveDataType#required} 上，对象引用类型则没有
         * 该概念），这里刻意不映射，以免悄悄改变前端渲染。
         */
//        private static com.alibaba.fastjson.JSONArray buildParameters(List<ActionParameterConfig> params) {
//            com.alibaba.fastjson.JSONArray parameters = new com.alibaba.fastjson.JSONArray();
//            for (ActionParameterConfig paramConfig : params) {
//                if (paramConfig == null) {
//                    continue;
//                }
//                com.alibaba.fastjson.JSONObject param = new com.alibaba.fastjson.JSONObject();
//                param.put("parameterId", paramConfig.identityValue());
//                param.put("displayName", paramConfig.displayName);
//                param.put("dataType", resolveDataType(paramConfig.dataType));
//                param.put("required", true);
//                param.put("hidden", paramConfig.hidden != null && paramConfig.hidden);
//
//                // 序列化约束配置
//                if (paramConfig.constraints != null) {
//                    param.put("constraints", serializeConstraints(paramConfig.constraints));
//                }
//
//                parameters.add(param);
//            }
//            return parameters;
//        }

//        /**
//         * 数据类型标识取子描述符的显示名（{@code "Primitive_Type"} / {@code "Object_Reference"}），
//         * 与本类两个常量逐字一致，前端拿到即可直接展示。
//         *
//         * <p>取不到描述符时（例如 dataType 缺省，或 TIS 尚未初始化）退回常量，
//         * 避免整个详情页打不开。
//         */
//        private static String resolveDataType(ActionParameterDataType dataType) {
//            if (dataType != null) {
//                var desc = dataType.getDescriptor();
//                if (desc != null) {
//                    return desc.getDisplayName();
//                }
//                return dataType instanceof ObjectReferenceDataType ? DATA_TYPE_OBJECT_REFERENCE : DATA_TYPE_PRIMITIVE;
//            }
//            return DATA_TYPE_PRIMITIVE;
//        }
        /**
         * 把前端 {@code Item.project()} 投影（{@code {impl, vals}}）经参数池现场生成的 descriptor
         * 解析、校验并执行。{@code executeActionRule} / {@code executeAction} 两分支共用，
         * 差异只在参数池来源（详情页编辑 vs 磁盘落盘），此处只做「参数池已就绪」之后的事。
         */
        private static void executePreviewForm(ActionType actionType, OntologyPluginMeta paramMeta,
                                               ActionParameters paramPool, JSONObject previewForm,
                                               boolean deleteConfirmed,
                                               IControlMsgHandler paramGetter, Context context) {
            Descriptor<?> formDesc = ActionFormDescriptorBuilder.buildDescriptor(paramMeta, paramPool);
            String impl = previewForm.getString(AttrValMap.PLUGIN_EXTENSION_IMPL);
            if (!formDesc.getId().equals(impl)) {
                paramGetter.addErrorMessage(context,
                        "提交的表单类型不匹配，期望:" + formDesc.getId() + "，实际:" + impl);
                return;
            }
            AttrValMap attrVals = AttrValMap.parseDescribableMap(
                    Optional.empty(), previewForm, formDesc, PropValRewrite.dftRewrite());
            // 必须用 validateWithScope：validate() 不会写入 plugin/item 位置，
            // 而 PluginValidateResult 的构造器要求这两个值非 null。
            PluginValidateResult validateResult =
                    attrVals.validateWithScope(paramGetter, context, 0, 0, FormVaildateType.STRICT);
            if (!validateResult.isValid()) {
                return;
            }
            actionType.executeActionRule(validateResult.newInstance(), deleteConfirmed);
        }

        @Override
        public void appendExternalProps(com.alibaba.fastjson.JSONObject multiStepsCfg) {
            // 可以在这里添加额外的配置信息到前端
        }

        @Override
        public OntologyEnum getOntologyType() {
            return OntologyEnum.Action;
        }

        @Override
        public String getDisplayName() {
            return "Action";
        }

        @Override
        public String shortComment() {
            return "定义本体对象的操作类型，包括写操作、通知、Webhook 等";
        }
    }
}
