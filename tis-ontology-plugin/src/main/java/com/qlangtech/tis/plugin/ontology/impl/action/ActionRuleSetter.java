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
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.OneStepOfMultiSteps;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.IPluginStore;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.Ontology;
import com.qlangtech.tis.plugin.ontology.OntologyAction;
import com.qlangtech.tis.plugin.ontology.impl.OntologyPluginMeta;
import com.qlangtech.tis.plugin.ontology.impl.action.type.ActionType;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.util.IPluginContext;
import org.apache.commons.lang3.ArrayUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Action Type Step 2: 规则设置
 * <p>
 * 配置 Action 的执行规则。<b>一个 Action 可以组合多条规则</b>（Palantir:
 * "Action types can include combinations of Ontology rules."），列表顺序即执行顺序；
 * 非法组合由 {@link RuleCombinationValidator} 在提交时拦截。
 * <p>
 * 建模指导：Action 是唯一的执行与 UI 单元 —— 提交时整个 Action 的全部规则在一个事务内
 * 原子执行，前端只呈现一颗提交按钮（删除类 Action 呈现为红色危险按钮，见
 * {@code DefaultOntologyAction#resolveDrivingRule} 危险优先推导），不存在「只执行某条规则」。
 * 因此<b>独立的删除操作请单独建 Action</b>（只含 DeleteObjectRule）；同主键
 * create+delete 组合虽合法（按顺序编译），但通常没有业务意义。混合规则的合法场景是
 * 跨对象类型的组合业务操作（如「下单」= 建订单 + 建订单行 + 改库存）。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/6
 * @see RuleCombinationValidator
 */
public class ActionRuleSetter extends OneStepOfMultiSteps {

    /**
     * 规则列表。用 {@code MULTI_DESCRIBLE_PLUGIN} 渲染成一张表，每行是一个
     * {@link ActionType} 子类实例。
     *
     * <p>注意<b>不要</b>再加 {@code @SubForm}：type-13 的嵌套形态完全由
     * {@code desClazz} 决定，{@code @SubForm} 是给「列表里放纯配置对象」用的另一套机制，
     * 两者叠加会让表单初始化走错分支。
     */
    @FormField(ordinal = 0, type = FormFieldType.MULTI_DESCRIBLE_PLUGIN,
            desClazz = ActionType.class, validate = {Validator.require})
    public List<ActionType> actionTypes;

    /**
     * 读取本 Action 已持久化的规则列表。
     *
     * <p>与 {@link ActionParameters#loadParameters(OntologyPluginMeta)} 同一套调用约定
     * （{@code pluginIdVal} 非空的 meta、不去线程上下文取），但落盘位置不同：规则是
     * {@link com.qlangtech.tis.plugin.ontology.impl.action.DefaultOntologyAction} 向导的
     * <b>第二步</b>，与 Metadata 一起落在
     * {@code <cfgRoot>/ontology/<domain>/action-type/<pluginIdVal>.xml} 里，
     * 没有独立的 params 目录，所以只能整体加载 Action 再从步骤数组里取。
     *
     * <p>返回的是「拷贝出来的一个实例」而非磁盘上那个对象：详情页只读展示，
     * 调用方后续若拿去编辑/提交也不会改到缓存里的 Action 配置。
     *
     * @return 规则配置；Action 不存在或仍是两步向导之外的存量配置（没有 Rules 步骤）时返回
     * {@code null}，由调用方决定降级渲染
     */
    public static ActionRuleSetter loadRules(OntologyPluginMeta pluginMeta) {
//        OntologyAction action = Ontology.loadActionDetail(pluginMeta.getDomain(), pluginMeta.getPluginIdVal());
//        if (!(action instanceof DefaultOntologyAction defaultAction)) {
//            throw new IllegalStateException("must be type of " + DefaultOntologyAction.class.getSimpleName());
//        }

        DefaultOntologyAction defaultAction = DefaultOntologyAction.load(pluginMeta);

        ActionRuleSetter ruleSetter = new ActionRuleSetter();
        ruleSetter.actionTypes = new ArrayList<>(defaultAction.getActionTypes());
        return ruleSetter;
    }


    /**
     * 规则列表，永不为 null。
     */
    public List<ActionType> getActionTypes() {
        return this.actionTypes == null ? Collections.emptyList() : this.actionTypes;
    }

    /**
     *
     * @param pluginContext       插件上下文
     * @param currentCtx          当前上下文
     * @param preSavedStepPlugins 前置步骤插件实例数组
     */
    @Override
    public void processPreSaved(IPluginContext pluginContext, Context currentCtx,
                                OneStepOfMultiSteps[] preSavedStepPlugins) {
        // 保存当前步骤数据到上下文
        currentCtx.put(ActionRuleSetter.class.getName(), this);
        super.processPreSaved(pluginContext, currentCtx, preSavedStepPlugins);

        if (ArrayUtils.isEmpty(preSavedStepPlugins)) {
            synchronized (DefaultOntologyAction.class) {
                /**
                 * 说明在单独开启 {@link ActionRuleSetter} 插件的编辑操作
                 */
                OntologyPluginMeta pluginMeta = OntologyPluginMeta.createPluginMeta();
                DefaultOntologyAction defaultAction = DefaultOntologyAction.load(pluginMeta);
                defaultAction.setRuleSetter(this);

                // 规则整体落在 Action 的 XML 里（action-type/<pluginIdVal>.xml），没有独立的 rules 目录，
                // 故这里必须把「整个 Action」按同一 identity 覆盖写回：setPlugins 按 identity 替换
                // 已有条目并落盘，不会产生重复文件。
                OntologyPluginMeta saveMeta = OntologyPluginMeta.create(Ontology.OntologyEnum.Action, pluginMeta.getDomain())
                        .setPluginIdVal(pluginMeta.getPluginIdVal())
                        .setPersistence();
                // OntologyEnum.getPluginStore 的返回类型被擦成了 IPluginStore<Ontology>（见该枚举 line 390
                // 的实现），这里按 storeKeyGetter 的实际泛型回投
                @SuppressWarnings("unchecked")
                IPluginStore<OntologyAction> actionStore
                        = (IPluginStore<OntologyAction>) (IPluginStore<?>) Ontology.OntologyEnum.Action.getPluginStore(saveMeta);
                actionStore.setPlugins(pluginContext, Optional.of(currentCtx),
                        Collections.singletonList(new Descriptor.ParseDescribable<>(defaultAction)), true);
            }
        }
    }

    @TISExtension
    public static class Desc extends OneStepOfMultiSteps.BasicDesc {

        @Override
        public String getStepDescription() {
            return "Rules";
        }

        @Override
        public Step getStep() {
            return Step.Step2;
        }

        /**
         * 向导到本步为止：Parameters / SubmissionCriteria 两步已按 Palantir 文档移出主流程，
         * 改由 Action 详情页 Form 页签维护（参数池落 {@code params/<actionName>.xml}，
         * 提交条件后续接入同页签）。声明清单 {@code DefaultDesc#getStepDescriptionList()}
         * 已收敛为两步，这里若仍链到 Step3，前端向导会按每步保存响应里的
         * {@code nextStepPluginDesc} 继续走进 Step3/Step4，最终宿主保存提交 4 步，
         * 被 {@code OntologyAction#setSteps} 的长度断言拦下 —— 步骤链与声明清单必须同源。
         */
        @Override
        public Optional<BasicDesc> nextPluginDesc(OneStepOfMultiSteps current) {
            return Optional.empty();
        }

        @Override
        public boolean isFinalStep() {
            return true;
        }

        @Override
        protected boolean validateAll(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
            // 规则之间的组合约束（对象操作先后顺序、Function 规则互斥等）无法用逐字段的
            // Validator 表达，只能在这里兜。
            ActionRuleSetter ruleSetter = postFormVals.newInstance();
            List<ActionType> rules = ruleSetter.getActionTypes();
            if (rules.isEmpty()) {
                msgHandler.addFieldError(context, "actionTypes", "至少需要配置一条规则");
                return false;
            }
            Optional<String> err = RuleCombinationValidator.validate(rules);
            if (err.isPresent()) {
                msgHandler.addFieldError(context, "actionTypes", err.get());
                return false;
            }
            return super.validateAll(msgHandler, context, postFormVals);
        }
    }
}
