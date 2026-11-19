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
import com.qlangtech.tis.extension.impl.XmlFile;
import com.qlangtech.tis.plugin.IPluginStore;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.impl.OntologyPluginMeta;
import com.qlangtech.tis.util.IPluginContext;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Action Type Step 3: 参数配置
 * <p>
 * 定义 Action 的输入参数（全局参数池）。Parameters 是 ActionType 级别的全局变量，可以被多个 Rules 引用。
 *
 * <p>参数池由 {@link ActionParameterConfig} 结构化成子表单行，不再让用户手写 JSON 数组字符串。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/6
 * @see ActionParameterConfig
 * @see DefaultOntologyAction
 */
@SuppressWarnings("all")
public class ActionParameters extends OneStepOfMultiSteps {

    public static final String KEY_PARAMETERS = "parameters";

    @FormField(ordinal = 0, type = FormFieldType.MULTI_DESCRIBLE_PLUGIN,
            desClazz = ActionParameterConfig.class, validate = {Validator.require})
    public List<ActionParameterConfig> parameters;

    /**
     * 自定义提交按钮文案（对应 Palantir 表单设计器里的 <i>Customize submit button</i>）。
     *
     * <p>多态字段，两个子类：{@link NoneCustomizeContent}
     * （{@code "off"}，用缺省的 {@code Submit}）与
     * {@link DefaultCustomizeContent}
     * （{@code "default"}，带上自定义文案）。{@code .json} 里的 {@code dftVal} 是 {@code "off"}。
     */
    @FormField(ordinal = 1, validate = {Validator.require})
    public CustomizeContent submitButton;

    /**
     * 自定义执行成功提示（对应 Palantir 表单设计器里的 <i>Customize success message</i>）。
     *
     * @see #submitButton
     */
    @FormField(ordinal = 2, validate = {Validator.require})
    public CustomizeContent successMessage;

    /**
     * 读取本 Action 已持久化的参数池。
     *
     * <p>参数文件落在
     * {@code <cfgRoot>/ontology/<domain>/action-type/params/<parameterId>/<pluginIdVal>.xml}：
     * 目录名是参数名，文件名取自 {@code pluginMeta.getPluginIdVal()}。因此调用方必须传入一个
     * {@code pluginIdVal} 非空的 meta（详情页链路里就是 Action 名），并且要与落盘时用的是
     * <b>同一个 meta</b>，否则会扫到别的 Action 留下的同名目录、却取不到自己的文件。
     *
     * <p>这里刻意不再从线程上下文取 meta：详情页请求的 extraParam 里没有 {@code identityName}，
     * 线程上下文 meta 会让 {@code getPluginStore} 直接抛
     * {@code param pluginIdVal can not be empty}。
     */
    public static ActionParameters loadParameters(OntologyPluginMeta pluginMeta) {
        return (ActionParameters) getActionParamsStore(pluginMeta).getPlugin();
    }

    /**
     * 参数池专用 store getter。
     *
     * <p>不能复用 {@code Ontology.OntologyEnum.Action.storeKeyGetter}：它的 pluginClass 是
     * {@code Ontology.class}，而本类不是 {@code Ontology} 子类，读写都会被类型校验拦下
     * （见 {@link ActionParamStoreGetter}）。
     */
    private static final ActionParamStoreGetter ACTION_PARAM_STORE_GETTER = new ActionParamStoreGetter();

    private void save(OntologyPluginMeta pluginMeta, IPluginContext pluginContext, Context context) {
        if (StringUtils.isEmpty(pluginMeta.getPluginIdVal())) {
            throw new IllegalArgumentException("ontology action name param can not be null");
        }
//        File actionParamsFile = Ontology.OntologyEnum.Action.storeKeyGetter.getAssistRootDir(
//                pluginMeta.getDomain(), Optional.of(ActionParameterConfig.KEY_ONTOLOGY_ACTION_PARAMS + File.separator + pluginMeta.getPluginIdVal()));

        // 随参数池一起把「参数池 → 扁平运行时表单」的产物
        // （form/<actionName>.groovy|.json）写到磁盘，执行期可 loadFromDisk 还原。
        // 先写表单再落参数池：参数名/order 等不合法时在持久化之前就失败，避免只落下半份配置。
        try {
            ActionFormDescriptorBuilder.writeToDisk(pluginMeta, this,
                    pluginMeta.getDomain(), pluginMeta.getPluginIdVal());
        } catch (Exception e) {
            throw new IllegalStateException(
                    "persist action form failed, action:" + pluginMeta.getPluginIdVal(), e);
        }

        IPluginStore<?> paramsStore = getActionParamsStore(pluginMeta);

        paramsStore.setPlugins(pluginContext, Optional.of(context), Collections.singletonList(new Descriptor.ParseDescribable(Collections.singletonList(this))), false);
    }

    private static IPluginStore<?> getActionParamsStore(OntologyPluginMeta pluginMeta) {
        IPluginStore<?> paramsStore
                = ACTION_PARAM_STORE_GETTER.getPluginStore(pluginMeta
                , Optional.of(ActionParameterConfig.KEY_ONTOLOGY_ACTION_PARAMS + File.separator + pluginMeta.getPluginIdVal() + XmlFile.KEY_XML_DOT_EXTENSION));
        return paramsStore;
    }

    /**
     * 供下游（例如提交条件的候选池）安全遍历，不必自己判空。
     */
    public List<ActionParameterConfig> getParameters() {
        return this.parameters == null ? Collections.emptyList() : this.parameters;
    }


    /**
     * 本步骤是「参数池 + 表单模板」的真正落盘点。
     *
     * <p>做两件事：
     * <ol>
     *   <li><b>发布上下文</b>：本步实例供第 4 步（{@code SubmissionCriterion.availableSubjects()}）
     *       的候选池使用；同时把第 2 步（{@code ActionRuleSetter}）也一并发布，
     *       因为向导首次渲染第 4 步时，只有第 3 步会在这里执行，第 2 步的
     *       {@code processPreSaved} 早已在别的请求里发生过。</li>
     *   <li><b>条件落盘</b>：见 {@link #shouldPersist(OneStepOfMultiSteps[])}。</li>
     * </ol>
     */
    @Override
    public void processPreSaved(IPluginContext pluginContext, Context currentCtx,
                                OneStepOfMultiSteps[] preSavedStepPlugins) {
        currentCtx.put(ActionParameters.class.getName(), this);
        // 第 4 步的候选池要拿目标对象类型，它挂在第 2 步的 ObjectActionType 上
        if (preSavedStepPlugins != null && preSavedStepPlugins.length > Step.Step2.getStepIndex()) {
            OneStepOfMultiSteps step2 = preSavedStepPlugins[Step.Step2.getStepIndex()];
            if (step2 != null) {
                currentCtx.put(step2.getClass().getName(), step2);
            }
        }

        if (shouldPersist(preSavedStepPlugins)) {
            this.save(pluginContext, currentCtx, preSavedStepPlugins);
        }

        super.processPreSaved(pluginContext, currentCtx, preSavedStepPlugins);
    }

    /**
     * 只有「正在保存本步」时才落盘，读取宿主时跳过。
     *
     * <p>判据是前置步骤数组的长度：
     * <ul>
     *   <li><b>向导保存第 3 步</b>：前置数组只有第 1、2 步（长度 2），本步不在其中 ⇒ 落盘；</li>
     *   <li><b>详情页 Form 页签单独保存</b>：没有多步骤历史，数组为空（长度 0）⇒ 落盘；</li>
     *   <li><b>向导保存第 4 步</b>：数组长度为 4，本步已在其中 ⇒ 跳过（第 3 步保存时已写过）；</li>
     *   <li><b>打开详情页</b>：{@code MultiStepsHostPluginFormProperties#getInstancePropsJson()}
     *       会对每个已存步骤调用 {@code processCurrentStep(..., allSteps)}，其长度恒等于
     *       <b>声明的步数</b>（4）⇒ 必须跳过，否则每次读都重写磁盘。</li>
     * </ul>
     */
    private static boolean shouldPersist(OneStepOfMultiSteps[] preSavedStepPlugins) {
        return preSavedStepPlugins == null
                || preSavedStepPlugins.length <= Step.Step3.getStepIndex();
    }

    /**
     * 落盘参数池与「参数池 → 扁平运行时表单」的产物。
     *
     * <p>actionName 的解析顺序：
     * <ol>
     *   <li>前置步骤里的 {@link ActionMetadata#name} —— <b>创建向导</b>唯一可靠的来源
     *       （创建请求的 extraParam 里没有 {@code identityName}）；</li>
     *   <li>线程上下文 meta 的 {@code pluginIdVal} —— 详情页 Form 页签保存时由
     *       {@code identityName_<action>} 提供；</li>
     *   <li>都没有则抛异常：静默写到错误路径比失败更糟。</li>
     * </ol>
     */
    private void save(IPluginContext pluginContext, Context currentCtx,
                      OneStepOfMultiSteps[] preSavedStepPlugins) {
        OntologyPluginMeta threadMeta = OntologyPluginMeta.createPluginMeta(currentCtx);
        String actionName = resolveActionName(preSavedStepPlugins);
        if (StringUtils.isEmpty(actionName)) {
            actionName = threadMeta.getPluginIdVal();
        }
        if (StringUtils.isEmpty(actionName)) {
            throw new IllegalStateException(
                    "无法确定当前 Action 的名称：前置步骤里没有 ActionMetadata，线程上下文也没有 pluginIdVal");
        }

        OntologyPluginMeta pluginMeta = threadMeta.setPluginIdVal(actionName).setPersistence();

        // 随参数池一起把「参数池 → 扁平运行时表单」的产物
        // （form/<actionName>.groovy|.json）写到磁盘，执行期可 loadFromDisk 还原。
        // 先写表单再落参数池：参数名/order 等不合法时在持久化之前就失败，避免只落下半份配置。
        try {
            ActionFormDescriptorBuilder.writeToDisk(pluginMeta, this,
                    pluginMeta.getDomain(), actionName);
        } catch (Exception e) {
            throw new IllegalStateException("persist action form failed, action:" + actionName, e);
        }

        IPluginStore<?> paramsStore = getActionParamsStore(pluginMeta);
        paramsStore.setPlugins(pluginContext, Optional.of(currentCtx),
                Collections.singletonList(new Descriptor.ParseDescribable(this)), false);
    }

    /**
     * 从前置步骤里找第一步的 Action 名称；创建向导链路里这是唯一可靠的来源。
     */
    private static String resolveActionName(OneStepOfMultiSteps[] preSavedStepPlugins) {
        if (preSavedStepPlugins == null
                || preSavedStepPlugins.length <= Step.Step1.getStepIndex()) {
            return null;
        }
        return preSavedStepPlugins[Step.Step1.getStepIndex()] instanceof ActionMetadata metadata
                ? metadata.name : null;
    }

    @TISExtension
    public static class Desc extends OneStepOfMultiSteps.BasicDesc {

        @Override
        public String getStepDescription() {
            return "Parameters";
        }

        @Override
        public Step getStep() {
            return Step.Step3;
        }

        @Override
        public Optional<BasicDesc> nextPluginDesc(OneStepOfMultiSteps current) {
            return Optional.of(new ActionSubmissionCriteria.Desc());
        }

        @Override
        public boolean isFinalStep() {
            return false;
        }
    }
}
