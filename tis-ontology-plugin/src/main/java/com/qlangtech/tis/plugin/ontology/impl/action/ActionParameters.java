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
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.IStoreSelfManagement;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.impl.XmlFile;
import com.qlangtech.tis.plugin.IPluginStore;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.impl.OntologyPluginMeta;
import com.qlangtech.tis.util.IPluginContext;
import com.qlangtech.tis.util.UploadPluginMeta;
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
public class ActionParameters implements Describable<ActionParameters>, IStoreSelfManagement {

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


    @Override
    public final void manipuldateProcess(IPluginContext pluginContext, UploadPluginMeta meta, Optional<Context> context) {
        OntologyPluginMeta pluginMeta = OntologyPluginMeta.createPluginMeta(meta);
        this.save(pluginMeta, pluginContext, context.orElseThrow());
    }

    @TISExtension
    public static class Desc extends Descriptor<ActionParameters> {
    }
}
