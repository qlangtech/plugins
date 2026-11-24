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
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.MultiStepsSupportHostDescriptor;
import com.qlangtech.tis.extension.OneStepOfMultiSteps;
import com.qlangtech.tis.extension.OneStepOfMultiSteps.Step;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.impl.PropValRewrite;
import com.qlangtech.tis.plugin.ds.JDBCTransactionScope;
import com.qlangtech.tis.plugin.ontology.Ontology;
import com.qlangtech.tis.plugin.ontology.OntologyAction;
import com.qlangtech.tis.plugin.ontology.impl.OntologyPluginMeta;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.CreateLinkRule;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.CreateObjectRule;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.DeleteLinkRule;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.DeleteObjectRule;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.FunctionBackedRule;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.ModifyObjectRule;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.OntologyActionRule;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.PropertyMappingConfig;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.RuleType;
import com.qlangtech.tis.plugin.ontology.impl.action.type.ActionType;
import com.qlangtech.tis.plugin.ontology.impl.action.type.LinkActionType;
import com.qlangtech.tis.plugin.ontology.impl.action.type.ObjectActionType;
import com.qlangtech.tis.plugin.ontology.impl.objtype.ObjectSetRowQuery;
import com.qlangtech.tis.runtime.module.misc.FormVaildateType;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.util.AttrValMap;
import com.qlangtech.tis.util.DefaultDescriptorsJSON;
import com.qlangtech.tis.util.DescribableJSON;
import com.qlangtech.tis.util.DescriptorsJSON;
import com.qlangtech.tis.util.IPluginContext;
import com.qlangtech.tis.util.RelevantFieldsContextAttrValMap;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

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

    public static DefaultOntologyAction load(OntologyPluginMeta meta) {
        OntologyAction action = Ontology.loadActionDetail(meta.getDomain(), meta.getPluginIdVal());
        if (!(action instanceof DefaultOntologyAction ontologyAction)) {
            throw new IllegalStateException("action must be type of " + DefaultOntologyAction.class.getSimpleName());
        }
        return ontologyAction;
    }


    @Override
    public void manipuldateProcess(IPluginContext pluginContext,
                                   com.qlangtech.tis.util.UploadPluginMeta pluginMeta,
                                   java.util.Optional<com.alibaba.citrus.turbine.Context> context) {
        // 可以在这里添加自定义持久化逻辑
        // 例如：验证跨步骤的业务规则、生成默认值等
    }

    /**
     * 按步骤枚举取已保存的子步骤实例。
     *
     * <p>不能再用 {@code getMultiStepsSavedItems()[idx]} 直接下标访问：存量 Action 是按
     * <b>两步</b>向导落盘的（Metadata + Rules），数组长度只有 2，读第 3/4 槽位会
     * {@code ArrayIndexOutOfBoundsException}，整个详情页打不开。这里统一返回 {@code null}
     * 让调用方各自决定降级语义。
     */
    private OneStepOfMultiSteps stepAt(Step step) {
        OneStepOfMultiSteps[] steps = this.stepsPlugin;
        int idx = step.getStepIndex();
        return (steps == null || steps.length <= idx) ? null : steps[idx];
    }

    /**
     * 获取第一步（Metadata）
     */
    private ActionMetadata getMetadata() {
        if (!(stepAt(Step.Step1) instanceof ActionMetadata metadata)) {
            throw new IllegalStateException("Action '" + this.getClass().getName()
                    + "' 缺少第一步 Metadata 配置，无法解析名称/显示名/描述");
        }
        return metadata;
    }

    private static final Step ACTION_RULE_SETTER_STEP = Step.Step2;

    /**
     * 获取第二步（Action Types）
     */
    private ActionRuleSetter getRuleSetter() {
        if (!(stepAt(ACTION_RULE_SETTER_STEP) instanceof ActionRuleSetter ruleSetter)) {
            throw new IllegalStateException("Action '" + this.getClass().getName()
                    + "' 缺少第二步 Rules 配置，无法解析规则列表");
        }
        return ruleSetter;
    }

    /**
     * 该 Action 配置的全部规则，顺序即执行顺序。
     */
    public List<ActionType> getActionTypes() {
        return getRuleSetter().getActionTypes();
    }

    /**
     * 把 Rules 步骤实例装回步骤数组（单独编辑 Rules 页签的保存路径用）。
     *
     * <p>不能走 {@link OntologyAction#setSteps(OneStepOfMultiSteps[])}：那里断言数组长度
     * 必须与 {@link DefaultDesc#getStepDescriptionList()} 声明的步数完全一致，而这里只
     * 换其中一个槽位。落盘数组长度以磁盘实际形态为准（两步向导是 2；四步向导时代落盘的
     * 存量是 4，多余槽位原样保留），这里只在下标越界时兜底扩长。
     */
    public void setRuleSetter(ActionRuleSetter ruleSetter) {
        Objects.requireNonNull(ruleSetter, "ruleSetter can not be null");

        final int idx = ACTION_RULE_SETTER_STEP.getStepIndex();
        OneStepOfMultiSteps[] steps = this.stepsPlugin;
        if (steps == null || steps.length < idx + 1) {
            // 与 getStepDescriptionList() 同源；同样直接 new 而非 getDescriptor()，编辑期不依赖扩展索引状态
            int expectLength = Math.max(new DefaultDesc().getStepDescriptionList().size(), idx + 1);
            OneStepOfMultiSteps[] grown = new OneStepOfMultiSteps[expectLength];
            if (steps != null) {
                System.arraycopy(steps, 0, grown, 0, steps.length);
            }
            steps = grown;
            this.stepsPlugin = grown;
        }
        steps[idx] = ruleSetter;
    }

//    /**
//     * 主规则（列表第一条）。
//     *
//     * <p>仅保留给「只能表达一条规则语义」的旧调用点（如 {@link #getLiteria()} 的展示）。
//     * 涉及执行与对象定位的逻辑必须遍历 {@link #getActionTypes()}，否则组合里的
//     * 第二条及之后的规则会被静默忽略。
//     */
//    public ActionType getActionType() {
//        return getActionTypes().stream().findFirst().orElse(null);
//    }

    /**
     * 第一个「修改 / 删除对象」类规则。这类规则需要对象身份参数来定位目标对象。
     */
    private Optional<ObjectActionType> findObjectRuleOfIdentity() {
        for (ActionType actionType : getActionTypes()) {
            if (actionType instanceof ObjectActionType objectActionType
                    && (objectActionType.getRule() instanceof ModifyObjectRule
                    || objectActionType.getRule() instanceof DeleteObjectRule)) {
                return Optional.of(objectActionType);
            }
        }
        return Optional.empty();
    }

    /**
     * 对象身份（rid）参数名：修改/删除类动作据此定位目标对象，返回对应表单字段名；
     * 创建类动作返回 null。仅供 {@link DefaultDesc#httpProcess} 的 {@code resolveRuntimeForm}
     * 分支回传字段名给前端预填，执行期仍由 {@code ObjectActionType#resolveRowIdentity} 精确解析。
     */
    public String resolveObjectIdentityParam(OntologyPluginMeta meta) {
        List<String> objectTypeRefs = ObjectActionType.objectTypeReferences(getActionTypes());
        return findObjectRuleOfIdentity()
                .map((objectActionType) -> ObjectActionType.resolveIdentityParameterName(
                        objectActionType.loadObjectType(meta.getDomain()),
                        ridParameterNameOf(objectActionType),
                        objectActionType.namespacePrefix(objectTypeRefs)))
                .orElse(null);
    }

    /**
     * 修改 / 删除对象规则上配置的「哪个参数携带目标对象标识」。
     *
     * <p>调用前必须已用 {@link #findObjectRuleOfIdentity()} 限定过规则类型。
     */
    private static String ridParameterNameOf(ObjectActionType objectActionType) {
        return objectActionType.getRule() instanceof ModifyObjectRule modifyRule
                ? modifyRule.ridParameterName
                : ((DeleteObjectRule) objectActionType.getRule()).ridParameterName;
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
        List<String> objectTypeRefs = ObjectActionType.objectTypeReferences(getActionTypes());
        return findObjectRuleOfIdentity()
                .map((objectActionType) -> objectActionType.loadObjectByRid(
                        meta.getDomain(), ridParameterNameOf(objectActionType), rid,
                        objectActionType.namespacePrefix(objectTypeRefs)))
                .orElse(null);
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
     * 获取第四步（提交条件）；存量两步 Action 没有该槽位，返回 {@code null}。
     */
    private ActionSubmissionCriteria getSubmissionCriteriaStep() {
        return stepAt(Step.Step4) instanceof ActionSubmissionCriteria criteria ? criteria : null;
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
     *
     * <p>存量两步 Action 没有 Step4 槽位，此时返回空列表而非抛异常 ——
     * 「没有条件」与「条件列表为空」语义相同，都是不限制提交。
     */
    public List<SubmissionCriterion> getSubmissionCriteria() {
        ActionSubmissionCriteria step = getSubmissionCriteriaStep();
        return step == null ? java.util.Collections.emptyList() : step.getCriteria();
    }

    @Override
    public java.util.List<com.qlangtech.tis.plugin.datax.transformer.UDFDesc> getLiteria() {
        java.util.List<com.qlangtech.tis.plugin.datax.transformer.UDFDesc> literia =
                com.google.common.collect.Lists.newArrayList();

        literia.add(new com.qlangtech.tis.plugin.datax.transformer.UDFDesc("Name", getName()));
        literia.add(new com.qlangtech.tis.plugin.datax.transformer.UDFDesc("Display Name", getDisplayName()));
        //literia.add(new com.qlangtech.tis.plugin.datax.transformer.UDFDesc("Target Object Type", getTargetObjectType()));
        // 一个 Action 可以组合多条规则，这里按配置顺序全部列出，用 " → " 表示执行次序
        String ruleGroups = getActionTypes().stream()
                .map((actionType) -> actionType.getRuleGroup().getDisplayName())
                .collect(java.util.stream.Collectors.joining(" → "));
        literia.add(new com.qlangtech.tis.plugin.datax.transformer.UDFDesc("Action Type", ruleGroups));

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
//                    new ActionParameters.Desc(),
//                    new ActionSubmissionCriteria.Desc()
            );
        }

        /**
         * 返回 Action 详情，供本体详情页的 Action Type 子页面（第二阶段编辑）使用。
         *
         * <p>响应体形状对应前端 {@code OntologyActionDetail}：
         * <pre>
         * {name, displayName, description, domain, createTime, objectType,
         *  actionTypes: [规则分组显示名],
         *  rules: [{ruleName, ruleType, ruleTypeLabel, target, properties[]}],
         *  parameters, descriptors, previewDescriptors,
         *  submissionCriteria: [{label, subjectKind, subjectRef, operator, value}]}
         * </pre>
         *
         * <p>{@code descriptors / parameters / previewDescriptors} 是「Form」页签的数据源，
         * 从 {@link ActionParameters#loadParameters} 读落盘的参数池 —— 参数池已移出创建向导，
         * 由本页签维护（落 {@code params/<actionName>.xml}），与规则列表解耦
         * （多条规则共享同一个参数池）。
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
            List<ActionType> actionTypes = defaultAction.getActionTypes();
            OntologyPluginMeta paramMeta = OntologyPluginMeta.create(Ontology.OntologyEnum.Action, ontologyName)
                    .setPluginIdVal(actionName)
                    .setPersistence();

            final com.alibaba.fastjson.JSONObject result = new com.alibaba.fastjson.JSONObject();
            switch (methodType) {
                case "getActionRulesRelevant" -> {
                    result.put("ruleDescs", DescriptorsJSON.desc(ActionRuleSetter.class));
                    ActionRuleSetter ruleSetter = ActionRuleSetter.loadRules(paramMeta);
                    if (ruleSetter != null) {
                        result.put("ruleSetter", new DescribableJSON(ruleSetter).getItemJson());
                    }
                    paramGetter.setBizResult(context, result);
                }
                case "getActionRelevant" -> {

                    // ---- Overview 四行属性：全部来自第一步 Metadata + 宿主本体元信息 ----
                    result.put("name", defaultAction.getName());
                    result.put("displayName", defaultAction.getDisplayName());
                    result.put("description", defaultAction.getDescription());
                    result.put("domain", ontologyName);
                    result.put("createTime", defaultAction.getCreate());
                    result.put("objectType", resolveObjectType(actionTypes));
                    result.put("actionTypes", actionTypes.stream()
                            .map((t) -> t.getRuleGroup().getDisplayName()).toList());

                    // ---- Rules 表 ----
                    result.put("rules", buildRules(actionTypes));

                    // ---- Submission Criteria 表（存量两步 Action 为空列表）----
                    result.put("submissionCriteria", buildSubmissionCriteria(defaultAction.getSubmissionCriteria()));

                    // ---- Form 页签依赖的三项 ----
                    ActionParameters paramPool = ActionParameters.loadParameters(paramMeta);
                    result.put("descriptors", new DefaultDescriptorsJSON(TIS.get().getDescriptor(ActionParameters.class)).getDescriptorsJSON());
                    if (paramPool != null) {
                        result.put("parameters", new DescribableJSON(paramPool).getItemJson());
                        result.put("previewDescriptors", ActionFormDescriptorBuilder.build(paramMeta, paramPool));
                    }
                    // 规则类型决定 Form 预览的提交语义（删除类 action 预览只显示红色删除按钮），
                    // 与 resolveRuntimeForm 下发给 workshop 模态框的同源同值
                    result.put("ruleType", resolveDrivingRuleTypeName(actionTypes));

                    paramGetter.setBizResult(context, result);
                }
                case "resolveActionForm" -> {
                    JSONObject post = pluginContext.getJSONPostContent();
                    // whitelist 必须是全部字段名（parameters / submitButton / successMessage / displayConfig），
                    // 否则没进 whitelist 的字段会静默读成 null（见 RelevantFieldsContextAttrValMap）。
                    RelevantFieldsContextAttrValMap<ActionParameters> attrs =
                            RelevantFieldsContextAttrValMap.createRelevantFieldsContext(paramGetter, context, post,
                                    new RelevantFieldsContextAttrValMap.RelevantFieldKeys(ActionParameters.KEY_PARAMETERS) {
                                        @Override
                                        protected List<String> toFieldValiateRelevantKeysCreator(Descriptor descriptor) {
                                            return new ArrayList<>(descriptor.getPropertyTypes().keySet());
                                        }
                                    });
                    if (!attrs.validate().isValid()) {
                        return;
                    }
                    ActionParameters params = attrs.createPluginInstance();

//                    com.alibaba.fastjson.JSONObject result = new com.alibaba.fastjson.JSONObject();
                    // 与 getActionRelevant 分支同构：把「参数池 → 扁平运行时表单」推导成的 descriptor 回传，
                    // 前端据此驱动右侧「表单预览」。
                    result.put("previewDescriptors", ActionFormDescriptorBuilder.build(paramMeta, params));
                    params.parameters = null;

                    result.put(ActionParameters.KEY_PARAMETERS, new DescribableJSON(params).getItemJson());

                    // 规则类型随表单 descriptor 一并刷新 Form 预览的提交语义（左侧改规则相关
                    // 字段后，删除类/普通提交的按钮形态在这里同步），与 getActionRelevant 同源
                    result.put("ruleType", resolveDrivingRuleTypeName(actionTypes));

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
                    // 删除确认标记与 executeAction 分支同构：详情页 Form 预览的删除按钮经前端
                    // interceptSave 拦截、确认框确认后再携带本标记提交。与 previewItem 同级，
                    // 不能塞进表单 vals（会被 STRICT 校验丢弃），DeleteObjectRule 凭它放行
                    boolean deleteConfirmed = post.getBooleanValue("deleteConfirmed");

                    RelevantFieldsContextAttrValMap<ActionParameters> attrs =
                            RelevantFieldsContextAttrValMap.createRelevantFieldsContext(paramGetter, context, paramsItem,
                                    new RelevantFieldsContextAttrValMap.RelevantFieldKeys(ActionParameters.KEY_PARAMETERS).addToFieldNewInstanceRelevantKeys());

                    if (!attrs.validate().isValid()) {
                        throw new IllegalStateException("attrs.validate() must be vaild");
                    }

                    ActionParameters paramPool = Objects.requireNonNull(attrs.createPluginInstance());// ActionParameters.loadParameters(paramMeta);
//                    if (paramPool == null) {
//                        paramGetter.addErrorMessage(context, "Action '" + actionName + "' 尚未配置参数池");
//                        return;
//                    }
                    executePreviewForm(actionTypes, paramMeta, paramPool, previewForm, deleteConfirmed, paramGetter, context);
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
                    executePreviewForm(actionTypes, paramMeta, paramPool, previewForm, deleteConfirmed,
                            paramGetter, context);
                }
                case "resolveLinkTargets" -> {
                    // 链接 Action 的目标对象下拉候选：按 targetObjectType 现查一批行。
                    // 参数走 query string（只两个标量），不投影 Action 树。
                    // 多规则下取列表里第一条链接规则：链接目标选择器只服务一条链接边。
                    LinkActionType linkActionType = actionTypes.stream()
                            .filter(LinkActionType.class::isInstance)
                            .map(LinkActionType.class::cast)
                            .findFirst()
                            .orElseThrow(() -> new IllegalStateException("Action '" + actionName
                                    + "' 未配置链接规则，无法解析链接目标"));
                    ActionParameters targetParamPool = ActionParameters.loadParameters(paramMeta);
                    // limit 缺失/非法交给 resolveTargetOptions 走默认值（夹取而非报错）
                    int limit = NumberUtils.toInt(paramGetter.getString("limit"), ObjectSetRowQuery.DEFAULT_MAX_ROWS);
                    paramGetter.setBizResult(context,
                            linkActionType.resolveTargetOptions(ontologyName, targetParamPool, limit));
                }
                case "resolveRuntimeForm" -> {
                    // workshop 按钮组打开运行时表单：参数池已落盘，直接加载并现场生成
                    // 「参数池 → 扁平运行时表单」的 descriptor 回传（与 resolveActionForm 同构），
                    // 再附上对象身份字段名供前端预填选中对象的 rid。
                    // com.alibaba.fastjson.JSONObject result = new com.alibaba.fastjson.JSONObject();
                    ActionParameters paramPool = ActionParameters.loadParameters(paramMeta);
                    result.put("previewDescriptors",
                            paramPool == null ? null : ActionFormDescriptorBuilder.build(paramMeta, paramPool));
                    // 显示配置（紧凑布局/表单宽度）扁平化后下发：off 或参数池未配置时为 null，
                    // 前端据此走框架缺省。刻意不走 ActionFormDescriptorBuilder 通道 ——
                    // 那条路受 contentHash 预览缓存约束，displayConfig 变更不会改 groovy/json 哈希。
                    result.put("displayConfig",
                            DisplayConfig.runtimeProps(paramPool == null ? null : paramPool.displayConfig));
                    result.put("ridFieldName", defaultAction.resolveObjectIdentityParam(paramMeta));
                    // 规则类型决定前端模态框的提交语义：DELETE_OBJECT / DELETE_LINK 时换成危险按钮
                    // 并携带 deleteConfirmed，其余规则保持普通「提交」。
                    // 多规则下取「最能代表本次提交危险程度」的那条，见 resolveDrivingRule。
                    result.put("ruleType", resolveDrivingRuleTypeName(actionTypes));

                    // 链接类 Action：回传源/目标对象类型与两个 RID 字段名，前端据此预填源对象、
                    // 渲染目标对象选择器。配置不完整（缺参数 / 链接类型坏）时只回传错误，
                    // 不能连坐整个表单的加载。链接选择器只服务一条链接边，取第一条链接规则。
                    LinkActionType linkActionType = actionTypes.stream()
                            .filter(LinkActionType.class::isInstance)
                            .map(LinkActionType.class::cast)
                            .findFirst()
                            .orElse(null);
                    if (linkActionType != null) {
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

        /**
         * 概览页的「Target Object Type」：取第一条带对象类型引用的对象规则。
         *
         * <p>没有独立的目标对象类型字段可读（{@code ActionMetadata#targetObjectType} 已注释、
         * 本体详情接口里的 objectType 同样注释），规则的 {@code objectTypeReference} 是<b>唯一</b>
         * 可用来源。一个 Action 的多条规则理论上可以指向不同对象类型，概览只展示第一条。
         */
        static String resolveObjectType(List<ActionType> actionTypes) {
            for (ActionType actionType : actionTypes) {
                if (actionType instanceof ObjectActionType objectActionType
                        && StringUtils.isNotEmpty(objectActionType.objectTypeReference)) {
                    return objectActionType.objectTypeReference;
                }
            }
            return null;
        }

        /**
         * 规则作用的目标：对象规则取对象类型名，链接规则取链接类型名，其余（函数、副作用、
         * 高级）没有统一的目标字段，退回规则分组的显示名。
         *
         * <p>这里走 {@code getRuleGroup().getDisplayName()} 而不是
         * {@code getDescriptor().getDisplayName()}：后者的唯一实现在
         * {@code BaseActionTypeDescriptor} 里是 {@code final} 且<b>就是</b>同一句取值，
         * 但 {@code getDescriptor()} 会触达 TIS 全局扩展索引 —— 详情页在索引未就绪时
         * 取目标字段不该失败，单测也不必拉起整个运行期。
         */
        static String resolveRuleTarget(ActionType actionType) {
            if (actionType instanceof ObjectActionType objectActionType) {
                return objectActionType.objectTypeReference;
            }
            if (actionType instanceof LinkActionType linkActionType) {
                return linkActionType.linkTypeReference;
            }
            return actionType.getRuleGroup().getDisplayName();
        }

        /**
         * 把规则列表转成前端 Rules 表的行数组，顺序即执行顺序。
         *
         * <p>抽成包级静态方法是为了能被单测直接驱动（不需要 TIS 运行期）：
         * 对 {@code ObjectActionType} / {@code LinkActionType} / {@code SideEffectActionType}
         * 三条常用路径都不会触达 {@code getDescriptor()}。
         *
         * <p>{@code rule} 为 {@code null}（规则尚未配置）时回退规则分组的枚举名，
         * 不抛异常 —— 半配置状态的 Action 也要能在详情页打开。
         */
        static com.alibaba.fastjson.JSONArray buildRules(List<ActionType> actionTypes) {
            com.alibaba.fastjson.JSONArray rules = new com.alibaba.fastjson.JSONArray();
            if (actionTypes == null) {
                return rules;
            }
            for (ActionType actionType : actionTypes) {
                if (actionType == null) {
                    continue;
                }
                OntologyActionRule rule = actionType.getRule();
                com.alibaba.fastjson.JSONObject row = new com.alibaba.fastjson.JSONObject();
                row.put("ruleName", actionType.ruleName);
                if (rule == null) {
                    row.put("ruleType", actionType.getRuleGroup().name());
                    row.put("ruleTypeLabel", actionType.getRuleGroup().getDisplayName());
                } else {
                    row.put("ruleType", rule.getRuleType().name());
                    row.put("ruleTypeLabel", rule.getRuleType().getDisplayName());
                }
                row.put("target", resolveRuleTarget(actionType));
                row.put("properties", collectRuleProperties(rule));
                rules.add(row);
            }
            return rules;
        }

        /**
         * 该规则会写哪些字段（Rules 表的 properties 列）。
         *
         * <p>未知规则类型不做处理、也不抛异常 —— 新增规则实现时这里最多是「少列几个字段」，
         * 不应该让整个详情页打不开。
         */
        static List<String> collectRuleProperties(OntologyActionRule rule) {
            List<String> properties = Lists.newArrayList();
            if (rule == null) {
                return properties;
            }

            if (rule instanceof CreateObjectRule createRule) {
                collectMappingTargets(createRule.propertyMappings, properties);
                // 自动生成 RID 时不需要用户提供 rid 参数
                if (!Boolean.TRUE.equals(createRule.generateRid)) {
                    addProperty(properties, "rid");
                }
            } else if (rule instanceof ModifyObjectRule modifyRule) {
                collectMappingTargets(modifyRule.propertyMappings, properties);
                addProperty(properties, modifyRule.ridParameterName);
            } else if (rule instanceof DeleteObjectRule deleteRule) {
                addProperty(properties, deleteRule.ridParameterName);
            } else if (rule instanceof CreateLinkRule createLinkRule) {
                addProperty(properties, createLinkRule.sourceObjectRidParam);
                addProperty(properties, createLinkRule.targetObjectRidParam);
            } else if (rule instanceof DeleteLinkRule deleteLinkRule) {
                addProperty(properties, deleteLinkRule.sourceObjectRidParam);
                addProperty(properties, deleteLinkRule.targetObjectRidParam);
            } else if (rule instanceof FunctionBackedRule functionRule) {
                // 函数规则的映射行 target 是函数形参名，同样计入「作用字段」供前端展示
                collectMappingTargets(functionRule.parameterMappings, properties);
            }
            // WebhookRule / NotificationRule 没有参数引用，无需处理
            // （NotificationRule 的收件人地址收进 NotifyChannel 各渠道子类静态配置）
            return properties;
        }

        private static void collectMappingTargets(List<PropertyMappingConfig> mappings, List<String> properties) {
            if (mappings == null) {
                return;
            }
            for (PropertyMappingConfig mapping : mappings) {
                if (mapping == null) {
                    continue;
                }
                addProperty(properties, mapping.target);
            }
        }

        private static void addProperty(List<String> properties, String propertyName) {
            if (StringUtils.isNotEmpty(propertyName) && !properties.contains(propertyName)) {
                properties.add(propertyName);
            }
        }

        /**
         * 把提交条件列表转成前端表格的行数组。
         *
         * <p>枚举一律下发<b>常量名</b>（{@code subjectKind} / {@code operator}），
         * 中文标签由前端映射 —— 前端本来就持有 {@code ComparisonOperator} 的展示口径，
         * 服务端再多发一份只会造成两处文案各改各的。
         */
        static com.alibaba.fastjson.JSONArray buildSubmissionCriteria(List<SubmissionCriterion> criteria) {
            com.alibaba.fastjson.JSONArray result = new com.alibaba.fastjson.JSONArray();
            if (criteria == null) {
                return result;
            }
            for (SubmissionCriterion criterion : criteria) {
                if (criterion == null) {
                    continue;
                }
                com.alibaba.fastjson.JSONObject row = new com.alibaba.fastjson.JSONObject();
                row.put("label", criterion.label);
                row.put("subjectKind", criterion.subjectKind == null ? null : criterion.subjectKind.name());
                row.put("subjectRef", criterion.subjectRef);
                row.put("operator", criterion.operator == null ? null : criterion.operator.name());
                row.put("value", criterion.value);
                result.add(row);
            }
            return result;
        }

        /**
         * 从规则列表里挑出决定「前端提交语义」的那一条。
         *
         * <p>前端 {@code action-form-modal.component.ts} 只有三个语义开关，都是从单条
         * {@code ruleType} 推出来的：{@code DELETE_OBJECT}（危险按钮 + deleteConfirmed）、
         * {@code DELETE_LINK}（同上，且按链接处理）、{@code CREATE_LINK}（按链接处理）。
         * 多规则下按危险程度降序取第一条匹配：
         * 删除 &gt; 链接 &gt; 其余，同等危险时取列表靠前的。
         * 链接规则优先于非链接规则，因为链接语义还带着 linkInfo 预填。
         */
        private static ActionType resolveDrivingRule(List<ActionType> actionTypes) {
            if (actionTypes == null || actionTypes.isEmpty()) {
                return null;
            }
            ActionType deleteRule = null;
            ActionType linkRule = null;
            for (ActionType actionType : actionTypes) {
                OntologyActionRule rule = actionType.getRule();
                RuleType ruleType = rule == null ? null : rule.getRuleType();
                if (ruleType == RuleType.DELETE_OBJECT || ruleType == RuleType.DELETE_LINK) {
                    deleteRule = actionType;
                    break;
                }
                if (linkRule == null && (actionType instanceof LinkActionType
                        || ruleType == RuleType.CREATE_LINK)) {
                    linkRule = actionType;
                }
            }
            if (deleteRule != null) {
                return deleteRule;
            }
            if (linkRule != null) {
                return linkRule;
            }
            return actionTypes.get(0);
        }

        /**
         * 「决定前端提交语义」那条规则的 RuleType 枚举名（{@link #resolveDrivingRule}
         * 危险优先推导），规则未配置时返回 null。
         *
         * <p>Form 页签预览（{@code getActionRelevant} / {@code resolveActionForm}）与
         * workshop 模态框（{@code resolveRuntimeForm}）三处经本方法下发，保证两条 UI 通道
         * 对同一个 Action 永远同源同值：删除类（DELETE_OBJECT / DELETE_LINK）呈现危险语义，
         * 提交时整个 Action 以一个事务执行（含混合规则里的创建/修改）。
         */
        private static String resolveDrivingRuleTypeName(List<ActionType> actionTypes) {
            ActionType drivingRule = resolveDrivingRule(actionTypes);
            OntologyActionRule rule = drivingRule == null ? null : drivingRule.getRule();
            return rule == null ? null : rule.getRuleType().name();
        }

        /**
         * 把前端 {@code Item.project()} 投影（{@code {impl, vals}}）经参数池现场生成的 descriptor
         * 解析、校验并执行。{@code executeActionRule} / {@code executeAction} 两分支共用，
         * 差异只在参数池来源（详情页编辑 vs 磁盘落盘），此处只做「参数池已就绪」之后的事。
         *
         * <p>按列表顺序逐条执行：前面规则的副作用（如创建对象）必须先于后面规则的写操作落地。
         * 全部规则跑在一个事务作用域内（见 {@link #executeActionTypes}），任一条失败即中断、
         * 后面的规则不再执行，且此前规则的写入整体回滚 —— 与 Palantir「按顺序编译成一次编辑」
         * 的原子语义一致。
         */
        private static void executePreviewForm(List<ActionType> actionTypes, OntologyPluginMeta paramMeta,
                                               ActionParameters paramPool, JSONObject previewForm,
                                               boolean deleteConfirmed,
                                               IControlMsgHandler paramGetter, Context context) {
            Descriptor<?> formDesc = ActionFormDescriptorBuilder.buildDescriptor(paramMeta, paramPool);
            String impl = previewForm.getString(AttrValMap.PLUGIN_EXTENSION_IMPL);
            if (!formDesc.getId().equals(impl)) {
//                paramGetter.addErrorMessage(context,
//                        "提交的表单类型不匹配，期望:" + formDesc.getId() + "，实际:" + impl);
                //   return;

                throw new IllegalStateException("is not match with form from submit,expect:" + formDesc.getId() + " but actual:" + impl);
            }
            if (MapUtils.isEmpty(formDesc.getPropertyTypes(true))) {
                throw new IllegalStateException("formDesc.getPropertyTypes can not be empty");
            }
            AttrValMap attrVals = AttrValMap.parseDescribableMap(
                    Optional.empty(), previewForm, formDesc, PropValRewrite.dftRewrite(), Function.identity());
            // 必须用 validateWithScope：validate() 不会写入 plugin/item 位置，
            // 而 PluginValidateResult 的构造器要求这两个值非 null。
            PluginValidateResult validateResult =
                    attrVals.validateWithScope(paramGetter, context, 0, 0, FormVaildateType.STRICT);
            if (!validateResult.isValid()) {
                return;
            }
            // 一次性解析出表单实例，各条规则共用：ActionFormPreview 是运行时临时对象，
            // 但每条规则都会各自遍历其属性，重复 newInstance 代价高且可能不一致。
            Describable<?> previewInstance = validateResult.newInstance();
            // 多对象类型 Action 的字段名带 <对象类型>_ 前缀，只有带上「本 Action 全部对象类型」
            // 规则才能判断自己是否该剥前缀（见 ObjectActionType#scopedPropertyVals）
            List<String> objectTypeRefs = ObjectActionType.objectTypeReferences(actionTypes);
            executeActionTypes(actionTypes, previewInstance, deleteConfirmed, objectTypeRefs);
        }

        /**
         * 把全部规则在一个事务作用域内按序执行（{@code executeActionInPreview} / {@code executeAction}
         * 两分支共用）。抽成包级静态方法是为了能被单测直接驱动（同 {@code buildRules} 的抽法）。
         *
         * <p>多步骤原子性：作用域内同一 jdbcUrl 的所有 DML 共享一条 {@code autoCommit=false} 的连接
         * （见 {@link JDBCTransactionScope}），全部规则成功后统一 commit；任一条失败即中断、后面的
         * 规则不再执行，且此前规则的写入整体回滚。规则的规则指向不同数据源时无法跨库原子（无 XA），
         * {@code commit()} 时各数据源按注册顺序各自提交。
         */
        static void executeActionTypes(List<ActionType> actionTypes, Describable<?> previewInstance,
                                       boolean deleteConfirmed, List<String> objectTypeRefs) {
            // 2026/11/24 执行前覆盖校验：映射行 typo 硬拦截 + CREATE 未覆盖属性告警
            // （见 ObjectActionType#validatePropertyCoverage），事务开启前 fail-fast
            ObjectActionType.validatePropertyCoverage(actionTypes, previewInstance, objectTypeRefs);
            try (JDBCTransactionScope tx = JDBCTransactionScope.open()) {
                for (ActionType actionType : actionTypes) {
                    actionType.executeActionRule(previewInstance, deleteConfirmed, objectTypeRefs);
                }
                tx.commit();
            }
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
